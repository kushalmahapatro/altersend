# Hyperdrive Interop Plan

The Electron/RN AlterSend worklet transfers file bytes via **Hyperdrive replication** over the same Hyperswarm connection that carries Protomux control messages. The Flutter+Rust app now speaks the same **Protomux control channel** (`altersend/control`) but still moves file bytes over a Rust-only `altersend/chunks` channel until Hyperdrive lands.

## Wire stack comparison

| Layer | Electron/RN (JS) | Flutter+Rust (today) |
|-------|------------------|------------------------|
| Discovery | Hyperswarm + `discoveryKey(topic)` | peeroxide (compatible) |
| Encryption | Noise (Hyperswarm) | Noise (peeroxide) |
| Multiplexing | Protomux | Protomux (`altersend_mux`) |
| Control | `altersend/control` JSON | `altersend/control` JSON ✅ |
| File bytes | Hyperdrive → hypercore replication | `altersend/chunks` (Rust peers only) |

## What works today

- **Rust ↔ Rust**: full transfer via Protomux control + chunk channel.
- **Rust ↔ JS (partial)**: control messages (`transfer-start`, `transfer-ready`, download events) can flow once both sides complete the Protomux handshake on connect. File bytes still require Hyperdrive.

## Remaining work

1. **`corestore.replicate(socket)`** — attach `hypercore-protocol` replication on each peer connection (`altersend_storage::ReplicationHandle`).
2. **Hyperdrive port** — no official Rust Hyperdrive crate exists. Options:
   - Port Hyperdrive v13 metadata/content core layout on top of `datrs/hypercore`.
   - Or embed the JS worklet for replication only (heavier).
3. **Sender staging** — mirror local files into outgoing Hyperdrive (`MirrorDrive` equivalent) and use real `drive.key` hex in `FileOffer.drive_key` (today it is random).
4. **Receiver download** — open remote Hyperdrive by `driveKey`, `update({ wait: true })`, stream file to disk.

## Crates added for interop

```
altersend_mux/      — minimal Protomux (wire-compatible with holepunchto/protomux)
altersend_storage/  — Corestore layout + hypercore-protocol replication hook
```

## Testing interop

When Hyperdrive lands, verify:

1. Flutter sender → Electron receiver
2. Electron sender → Flutter receiver
3. Late-join peer receives replayed `transfer-start` / `transfer-ready`
