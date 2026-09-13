# Changelog

## Unreleased

Performance and correctness pass across channels, the multiplexer, and the
pipeline. No public API changes.

### Highlights

- **Allocation-free channel fast paths.** `put!` and `take!` no longer
  allocate a closure per operation, and `take!` no longer allocates a result
  tuple or sequences it. JFR confirms the hot paths allocate no application
  objects.
- **Arbitrary values round-trip safely.** A value such as `[:block ...]` or
  `:closed` is returned unchanged by `take!`. Previously a value whose first
  element was `:block` was mistaken for an internal marker and crashed the
  take.
- **Much faster multiplexer.** The per-value virtual-thread fan-out and the
  per-value `Phaser` barrier were replaced by sequential blocking `put!` on
  the dispatcher thread. Every tap still receives every value, in order, with
  the same strict backpressure.
- **Pipeline `n=1` fix.** `nil` is the only EOF, so boolean `false` now passes
  through, and closing the output channel stops the loop.

### Compatibility

- No public API changes: protocols and function signatures are unchanged.
- **Multiplexer delivery timing.** Taps are now served sequentially in
  snapshot order rather than concurrently. Observed semantics are unchanged
  (all taps receive all values in order; a blocked tap blocks the mult), but
  the order in which different taps receive a given value is deterministic by
  tap snapshot position instead of racing. Code should not rely on cross-tap
  timing either way.
- **Pipeline `n=1`.** Boolean `false` inputs are no longer treated as EOF, and
  the loop stops when the output channel is closed.

### Performance

Measured on OpenJDK 25.0.4.1, Clojure 1.12.4, on Linux; min of N runs. The
multiplexer figures are an interleaved A/B against the previous revision on the
same machine. Absolute numbers vary by hardware.

Channels (200k `put!`/`take!` pairs, one producer thread and one consumer):

| Configuration    |   Before |    After |  Change |
|------------------|---------:|---------:|--------:|
| unbuffered       | 315.9 ms | 315.9 ms |      0% |
| buffered cap 5   |  89.7 ms |  89.9 ms |    ~0%  |
| buffered cap 100 |  32.0 ms |  21.9 ms | **-31%** |

Multiplexer (one source, buffered taps, one consumer per tap):

| Configuration              |   Before |    After |  Change |
|----------------------------|---------:|---------:|--------:|
| dispatch-only 5 taps, 50k  | 140.5 ms |  17.8 ms | **-87%** |
| dispatch-only 20 taps, 50k | 321.8 ms |  33.1 ms | **-90%** |
| full 5 taps, 100k          | 387.3 ms | 314.2 ms | **-19%** |
| full 20 taps, 25k          | 274.6 ms | 241.8 ms | **-12%** |

JFR (`settings=profile`) shows the multiplexer hot path no longer spends time
in `Phaser.internalAwaitAdvance` or ForkJoinPool/virtual-thread scheduling, and
the channel `put!`/`take!` fast paths allocate no application objects.

Correctness is covered by the full test suite: 28 tests, 792 assertions,
0 failures.

### Requirements / stack

- JDK 24 or later (tested on OpenJDK 25.0.4.1).
- Clojure 1.12.4.
- Test-only dependencies: `org.clojure/core.async` 1.7.701,
  `criterium/criterium` 0.4.6, `circleci/bond` 0.6.0, and the Cognitect
  test-runner `v0.5.1`.

### Running the tests and benchmarks

```bash
# Functional tests
clojure -M:test:test-runner

# Performance benchmarks (criterium quick-bench)
clojure -M:test:test-performance
```
