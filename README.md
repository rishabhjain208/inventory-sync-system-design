# Mars Cosmetics – Order & Inventory Sync Service (design)

Design for a service that syncs orders and stock across the website, Nykaa, Flipkart and Blinkit, and pushes confirmed orders to Microsoft Business Central.

## What's here

- [design/DESIGN.md](design/DESIGN.md): the design doc (answers all 8 questions, trade-offs, assumptions, Stretch A)
- [design/architecture.png](design/architecture.png): the architecture diagram, exported from [design/architecture.mmd](design/architecture.mmd) (Mermaid source)
- [design/reservation-pseudocode.md](design/reservation-pseudocode.md): pseudo-code for stock reservation and release (question 4)

## Setup

Nothing to install or run. This is a design exercise; the brief asks for a document, not code.

- Read [design/DESIGN.md](design/DESIGN.md) on GitHub. The diagram renders inline.
- To regenerate the diagram: `npx @mermaid-js/mermaid-cli -i design/architecture.mmd -o design/architecture.png`

## Approach

- **PostgreSQL is the single source of truth for stock.** Each reservation is one conditional `UPDATE … WHERE on_hand - reserved >= qty`, plus an append-only ledger row, in one transaction. When two channels sell the last unit at once, the row lock lets exactly one win.
- **Marketplaces see a buffered quantity** sized to how late we learn of their sales; the last units sell on the website, which reserves at checkout. This is what keeps cancellations under 0.5%.
- **Channels get absolute quantities**, coalesced per tick and sent within each channel's rate limit, so retries after timeouts or 429s are always safe.
- **All outbound work goes through a Postgres outbox** (ERP orders, channel cancels), so an ERP outage delays orders but never loses or duplicates them.
- **One NestJS codebase, three process roles** (api, worker, scheduler) on ECS Fargate, with per-channel queues for isolation. About ₹34k/month, within the ₹50k budget.
