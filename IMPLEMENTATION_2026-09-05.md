# Implementation and verification — 2026-09-05

> Historical observations from September 5, 2026; findings, line references and
> runtime status below describe that date. Use [OPERATIONS.md](OPERATIONS.md)
> and the active project READMEs for current behavior.

The approved fixes are implemented and deployed to the local Docker stack. New
live entries were re-enabled at **11:49:21 UTC** after regression and rollover
verification. At 11:52:56 UTC, all eight services were healthy, the deployment
pause and maintenance marker were absent, and no independent safety stop was
active. Existing strategy parameters and pre-existing VPS configuration edits
were preserved.

## Result

| Area | Corrected behavior |
|---|---|
| Contract and model | Explicit market/token/fee metadata; trailing-60-second average forecast; v5 timestamps; no terminal-CDF substitution for TWAP; two-second entry freshness limits. |
| Settlement | Resolved token payouts determine cash accounting. Explicit outcome indices support bounded Polygon ConditionalTokens reads when CLOB winner metadata lags. Missing payouts remain pending without blocking current-market discovery. |
| Live entry submission | Signed identity and durable intent precede POST. Unknown acceptance blocks another buy and survives restart. Eligibility is checked again after signing and durable persistence. |
| Partial fills | Residual cancellation and subsequent fills/balances are reconciled before adopting a position or clearing an intent. Confirmed small modern fills remain tracked for resolution. |
| Live fees and accounting | Matching V2 receipt amounts provide fee-inclusive cash flows. Newly tagged tentative costs can be refined; historical records are not silently repriced. Cash, trading P&L and dated equity are separate. |
| Stops and recovery | Intentional stops and safety latches persist. Downstream recovery requires healthy dependencies; backoff is bounded. Management includes spot and the restricted logs adapter. |
| Grid execution | Finalized entries use current evidence and the selected execution cost. Independent instances still enforce their own available quantity. Fees apply per price level within the order budget. |
| Grid history and reporting | Incremental statistics and bounded asynchronous reporting replace repeated full-history scans. CSV recovery rejects mixed experiments. Decision and execution times are separate. |
| Dashboard | Growing/replaced CSV corrections and voids reseed coherently; collectors own change detection; live liveness uses live-owned heartbeats; reconnects recover snapshots. The dashboard no longer has direct Docker socket access. |

The final runtime check crossed the 11:45 UTC boundary. Grid discovery advanced
at **11:45:02** and live discovery at **11:45:05**. Across 300 samples spanning the
transition, grid intervals were **1.49 seconds p95** and **1.96 seconds p99**;
post-resume measurements were **1.37 seconds p95** and **1.67 seconds p99**.
Targets were p95 ≤2 seconds and p99 ≤5 seconds.

Measured resource changes are limited to calc at two CPUs/two Numba threads and
grid at one CPU. A 300,000-path, 600-second-horizon TWAP calculation measured
0.921 seconds at one CPU and 0.457 seconds at two. The separate 1296-instance
synthetic grid benchmark measured p95 processing of 4.000 seconds at 0.1 CPU and
0.319 seconds at one CPU; that benchmark excluded model/network latency.

## Tests and evidence

| Check | Final result |
|---|---|
| Full trader/model/grid suite in the final Docker image, with isolated Redis and public-feed checks | **957 passed, zero skipped** |
| Full dashboard/backend/management suite | **148 passed** |
| Frontend store/reconnect behavior | **4 passed** |
| TypeScript/Vite build and mandatory Python undefined-name checks | Passed |
| Updated frontend dependency audit | Zero reported vulnerabilities |
| Six deliberately broken safety variants | All detected by their regression tests |
| Local Docker rebuild and complete stack recreation | Passed; eight services healthy |

Test cleanup removed or consolidated direct self-checks, superficial constructor
assertions and duplicate coverage. Ineffective rejection tests now have otherwise
valid inputs. File-I/O tests measure actual reads, clock tests advance time, and
accounting tests require real pathway events and independently specified amounts.
The mutation checks cover order uncertainty, safety latches, stale staged
probabilities, selected execution cost, independent TP depth and final freshness.

Numerical checks include antithetic-pair uncertainty, constant-price and limiting
cases, an independent Gaussian average-price benchmark, and timestep/path-count
refinement. A recorded public-market fixture exercises MC → v5 → feed → paper
decision → persisted resolution. It correctly rejects stale historical evidence
and invents no fills; missing historical forward/depth are disclosed in the fixture.

Runtime verification exposed two additional settlement integration gaps. Pending
legacy records now acquire explicit contract identity before resolution, and
delayed settlement indexing no longer pins the active feed. Restoration uses the
canonical SQLite rows and reads only JSON records absent from the database,
avoiding thousands of redundant startup file reads.

## Preserved data

The original paper experiment was archived at 10:01:20 UTC. Its 1296 instances
had **zero unfinished positions** at the boundary. The corrected experiment is
`twap_v5_20260905`, initialized with **$1000 per independent paper instance**.

Private paper/live migration snapshots were retained locally. Account balances
and trade counts are omitted from this public report. The live trade CSV was
unchanged throughout deployment verification.

One market-only settlement row processed through the old startup fallback was
corrected using verified on-chain payouts: market `1788602400`, DOWN=1 and UP=0.
Its inferred final BTC price was cleared. An audit copy of the pre-correction
record is retained in the paper archive. This did not change live trades or equity.

## Evidence boundaries

The TWAP forecast approximates a continuous arithmetic average; it does not
reproduce unpublished Chainlink sampling details or establish predictive edge.
Published oracle observations and resolved token payouts remain independent
measurements. Slow source observations still cause entries to be skipped under
the two-second limits; timestamps were not refreshed artificially to enable trades.

Historical cash and starting-capital-plus-trading-P&L differed. The UI
now presents their distinct sources instead of treating that difference as
trading profit. This work did not attribute every historical cash movement or
rewrite the old ledger to force agreement. VPS/offload execution was not deployed
or exercised; the validated runtime is the local stack.

Operational commands and safety-stop semantics are documented in
[OPERATIONS.md](OPERATIONS.md).
The [original audit](AUDIT_2026-09-05.md)
is retained as the pre-fix evidence.
