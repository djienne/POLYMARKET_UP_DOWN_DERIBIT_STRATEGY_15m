# Historical short-TTM fixtures

These JSON captures came from a Bitcoin **daily, terminal-price** market on
March 26, 2026. They are regression inputs for legacy Heston/SSVI calculations,
not benchmarks for the current 15-minute trailing-average contract.

- `terminal_data_heston_short_ttm.json` retains an extreme fitted Heston surface
  and a numerically suspect density-based probability near expiry.
- `terminal_data_ssvi_short_ttm.json` retains a separate SSVI comparison capture.
  Its horizon and observations differ; the two files are not a controlled
  same-input comparison.

Use the stored fields for exact parameters and outputs. An options fit with a
high R-squared does not validate an extrapolated digital probability. Market
quotes and another model's output are comparison evidence, not the true event
probability. First-derivative/characteristic-function calculations, density
normalization, bounds, limiting cases and numerical convergence provide separate
checks of the legacy numerical methods.

Current TWAP validation lives in `test_twap_mc.py`, `test_twap_contracts.py`,
`test_twap_pipeline.py` and `test_recorded_twap_market.py` in the parent tests
folder. Those checks cover the average payoff, source-time freshness and the
actual recorded-market path; they do not claim an observed strategy edge.
