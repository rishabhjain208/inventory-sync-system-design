# Stock reservation: pseudo-code

TypeScript-style pseudo-code for NestJS with `pg`. Every step runs inside **one** Postgres transaction at READ COMMITTED.

```ts
async function handleOrderCreated(evt: NormalizedOrderEvent) {
  try {
    await db.tx(async (t) => {
      // 1. Idempotent upsert, then lock the order row so all events for this order run one at a time.
      await t.query(
        `INSERT INTO orders (id, channel, external_order_id, status, payment_mode, total_amount, shipping_pincode)
         VALUES (gen_random_uuid(), $1, $2, 'NEW', $3, $4, $5)
         ON CONFLICT (channel, external_order_id) DO NOTHING`,
        [evt.channel, evt.externalOrderId, evt.paymentMode, evt.totalAmount, evt.pincode]);

      const order = await t.one(
        `SELECT * FROM orders WHERE channel = $1 AND external_order_id = $2 FOR UPDATE`,
        [evt.channel, evt.externalOrderId]);

      // Duplicate webhook, already reserved, or a CANCELLED tombstone created by an earlier out-of-order event.
      if (order.status !== 'NEW') return;

      // 2. Reserve every line. Sorting by SKU means two multi-line orders always lock rows in the same order (no deadlock).
      const lines = mergeDuplicateSkus(evt.items).sort((a, b) => a.sku.localeCompare(b.sku));

      for (const line of lines) {
        const row = await t.oneOrNone(
          `UPDATE inventory
              SET reserved = reserved + $2, version = version + 1, updated_at = now()
            WHERE sku = $1 AND on_hand - reserved >= $2      -- availability check and write are one atomic statement
        RETURNING on_hand, reserved`,
          [line.sku, line.qty]);
        // If another tx holds the row lock, this UPDATE waits. After that tx commits,
        // Postgres re-checks the WHERE against the new row, so we never read a stale value.

        if (!row) throw new OutOfStock(line.sku);           // rolls back every line reserved so far

        await t.query(
          `INSERT INTO stock_ledger (sku, delta_on_hand, delta_reserved, on_hand_after, reserved_after, reason, ref_type, ref_id, actor)
           VALUES ($1, 0, $2, $3, $4, 'RESERVE', 'ORDER', $5, 'system')`,
          [line.sku, line.qty, row.on_hand, row.reserved, order.id]);

        await t.query(
          `INSERT INTO order_items (order_id, sku, qty, unit_price, reserved) VALUES ($1, $2, $3, $4, true)`,
          [order.id, line.sku, line.qty, line.unitPrice]);
      }

      await t.query(`UPDATE orders SET status = 'RESERVED' WHERE id = $1`, [order.id]);

      // 3. Outbox: the ERP job is committed atomically with the reservation, so it can never be lost.
      await t.query(
        `INSERT INTO sync_jobs (type, ref_id, status, idempotency_key)
         VALUES ('ERP_CREATE', $1, 'PENDING', 'erp:create:' || $1)
         ON CONFLICT (idempotency_key) DO NOTHING`,
        [order.id]);
    });
    // After commit: inventory.version > channel_stock.pushed_version, so the stock dispatchers pick the change up on their next tick.
  } catch (e) {
    if (!(e instanceof OutOfStock)) throw e;               // infrastructure error: BullMQ retries the whole job (idempotent)
    await rejectOrder(evt, e.sku);                          // separate tx: status REJECTED_OOS, CHANNEL_CANCEL job, metric++
  }
}

async function handleOrderCancelled(evt: NormalizedOrderEvent) {
  await db.tx(async (t) => {
    // Upsert as a CANCELLED tombstone: if CREATED arrives later, it sees a terminal state and does nothing.
    await t.query(
      `INSERT INTO orders (id, channel, external_order_id, status) VALUES (gen_random_uuid(), $1, $2, 'CANCELLED')
       ON CONFLICT (channel, external_order_id) DO NOTHING`, [evt.channel, evt.externalOrderId]);
    const order = await t.one(`SELECT * FROM orders WHERE channel = $1 AND external_order_id = $2 FOR UPDATE`,
                              [evt.channel, evt.externalOrderId]);
    if (order.status === 'REJECTED_OOS') return;            // nothing was reserved
    if (order.status === 'DISPATCHED') return markReturnPending(order); // stock comes back only after warehouse QC

    // Release only the lines still reserved, so running this twice releases nothing the second time.
    const items = await t.many(
      `UPDATE order_items SET reserved = false WHERE order_id = $1 AND reserved RETURNING sku, qty`, [order.id]);
    for (const it of items.sort(bySku)) {
      const row = await t.one(
        `UPDATE inventory SET reserved = reserved - $2, version = version + 1, updated_at = now()
          WHERE sku = $1 RETURNING on_hand, reserved`, [it.sku, it.qty]);
      await insertLedger(t, it.sku, 0, -it.qty, row, 'RELEASE_CANCEL', order.id);
    }
    await t.query(`UPDATE orders SET status = 'CANCELLED' WHERE id = $1`, [order.id]);
    await enqueueErpCancelIfCreateExists(t, order.id);     // depends_on = ERP_CREATE job
  });
}
```

Notes:
- The marketplace buffer **B** is *not* part of the `WHERE` clause. A marketplace has already accepted that order, so we fulfil it whenever there is physical stock. B only reduces the quantity we *show* on marketplaces.
- The website's checkout hold uses the same `UPDATE`, with `status = 'HELD'` and `hold_expires_at = now() + 15 min`. The sweeper releases expired holds through the cancel path.
