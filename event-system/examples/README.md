# Event-system example

[basic.rs](basic.rs) creates a stream for a fixed-size `JobEvent` enum and sends
`Started` and `Finished` events. A typed subscriber decodes Rust values; a dynamic
subscriber reads variant names and fields from the published schema.

Run on Linux from the repository root:

```bash
cargo run --locked -p agave-event-system --example basic
```

Output:

```text
Typed: Started { job_id: 42 }
Dynamic: Started
  job_id = U64(42)
Typed: Finished { job_id: 42 }
Dynamic: Finished
  job_id = U64(42)
```

Everything runs in one process. Both subscribers connect before events are sent,
and the temporary directory is cleaned up when the example exits.
