# Legacy AlterSend Peer Interop

The original AlterSend app (Electron desktop + React Native mobile) transfers file bytes via **Hyperdrive replication** over Hyperswarm connections that also carry Protomux control messages. This Flutter+Rust app targets the same network and control protocol, with incremental wiring for legacy peers.

## Wire stack

| Layer | Legacy JS worklet | Flutter+Rust (today) |
|-------|-------------------|----------------------|
| Discovery | Hyperswarm + `discoveryKey(topic)` | peeroxide ✅ |
| Encryption | Noise (Hyperswarm) | Noise (peeroxide) ✅ |
| Multiplexing | Protomux | `altersend_mux` ✅ |
| Control | `altersend/control` JSON | `altersend/control` JSON ✅ |
| File bytes | `hypercore/alpha` + Hyperdrive | `altersend/chunks` (Rust peers) |

## Legacy replication channel

JS `corestore.replicate(socket)` attaches Hypercore replication on the shared Protomux mux:

```
protocol:  "hypercore/alpha"   (alias: "hypercore")
id:        <32-byte discovery key = BLAKE2b(publicKey)>
handshake: capability + seeks flag
messages:  sync, request, data, want, range, …
```

Flutter+Rust now **detects** when a connected peer opens `hypercore/alpha` and marks the peer as `Legacy`.

## What works today

| Scenario | Status |
|----------|--------|
| Flutter ↔ Flutter (Rust) | Full transfer via control + `altersend/chunks` ✅ |
| Control messages with legacy peer | `transfer-start`, `transfer-ready`, download telemetry ✅ |
| Per-topic Noise identity (join) | `identity/topic-keys.json` — JS-compatible ✅ |
| Legacy peer detection | Opens `hypercore/alpha` → `PeerInteropMode::Legacy` ✅ |
| Outgoing core registration | Hypercore staged + registered for replication hooks ✅ |
| Legacy file byte transfer (sender) | Hypercore replication wire handler serves staged blocks ✅ |
| Legacy file byte transfer (receiver) | Hyperdrive pull path: replicate metadata+blobs, read via hyperbee scan ✅ (needs outgoing Hyperdrive sender) |

## Implemented setup (this branch)

1. **`PeerIdentityStore`** — persists per-topic Noise keypairs (`topic-keys.json`), swapped into peeroxide on `join()` like the JS worklet.
2. **`PeerInteropMode`** — `Rust` vs `Legacy` vs `Unknown`; detected from remote protomux channel opens.
3. **`ReplicationRegistry`** — tracks outgoing Hypercore public/discovery keys per peer session.
4. **Sender path** — legacy download-request telemetry is accepted; chunk streaming is skipped (legacy peers pull via replication).
5. **Receiver path** — when a legacy sender is detected, opens `IncomingHyperdrive` by `driveKey`, registers metadata+blobs cores for download replication, proactively opens `hypercore/alpha` channels, and reads the file via hyperbee scan + blob blocks.
6. **`HypercoreReplicationClient`** — download-side replication (sync/request/data apply) for incoming cores.
7. **`IncomingHyperdrive`** — minimal hyperbee reader: path lookup, header content-key → blobs core, `read_file()`.

## Remaining work for full legacy interop

1. **Outgoing Hyperdrive port** — stage files into real Hyperdrive metadata/content cores (not bare Hypercore blocks) so legacy receivers can pull by `driveKey`.
2. **End-to-end manual interop** — Flutter sender → legacy receiver and legacy sender → Flutter receiver on real devices.

## Crates

```
altersend_mux/      — Protomux (control + hypercore/alpha detection)
altersend_storage/  — Hypercore staging, replication registry, IncomingHyperdrive reader
altersend_p2p/      — identity store, peer mode, swarm/orchestrator wiring
```

## Manual interop test

1. Legacy sender → Flutter receiver (receiver pull path implemented; verify on device)
2. Flutter sender → legacy receiver (blocked until outgoing Hyperdrive port)
3. Late-join peer receives replayed `transfer-start` / `transfer-ready`
