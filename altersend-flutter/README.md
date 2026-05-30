# AlterSend Flutter + Rust

A reimplementation of [AlterSend](https://github.com/denislupookov/altersend) with **Flutter as a thin UI shell** and **Rust as the application spine** — business logic, session state, and P2P networking all live in Rust. The Flutter layer handles rendering, file picking, and platform integration only.

## Architecture

```
┌─────────────────────────────────────────┐
│  Flutter (app/)                         │
│  Material UI · file_picker · clipboard  │
│  Polls JSON UI snapshot from Rust       │
└──────────────────┬──────────────────────┘
                   │ flutter_rust_bridge
┌──────────────────▼──────────────────────┐
│  altersend_ffi                            │
└──────────────────┬──────────────────────┘
┌──────────────────▼──────────────────────┐
│  altersend_engine                         │
│  Session commands · event → reducer       │
└──────────────────┬──────────────────────┘
┌──────────────────▼──────────────────────┐
│  altersend_domain                         │
│  Pure reducer · join codes · UI models    │
└──────────────────┬──────────────────────┘
┌──────────────────▼──────────────────────┐
│  altersend_p2p                            │
│  peeroxide (Hyperswarm) · control channel │
└───────────────────────────────────────────┘
```

This mirrors the original stack (`packages/domain` + `packages/core` worklet + app shells), but collapses domain and networking into Rust instead of TypeScript + Bare.

## Features (parity with original)

| Feature | Status |
|---------|--------|
| Send: pick files → join code | ✅ |
| Receive: enter 64-char code → connect | ✅ |
| Hyperswarm topic discovery (peeroxide) | ✅ |
| Control protocol (`transfer-start`, `transfer-ready`, download events) | ✅ |
| Session state machine (ported reducer) | ✅ |
| File bytes over P2P (chunked wire protocol) | ✅ |
| Peer watchdog (receiver timeout) | ✅ |
| Hyperdrive replication (interop with Electron/RN app) | 🚧 Roadmap |
| QR scan / deep links / Photos save | 🚧 Platform plugins |

## Prerequisites

- **Rust** 1.85+ (2024 edition)
- **Flutter** 3.22+ stable
- Linux/macOS/Windows for desktop; mobile toolchains for iOS/Android

## Quick start

```bash
# From repo root
cd altersend-flutter

# Regenerate FFI bindings (after changing rust/altersend_ffi/src/api.rs)
flutter_rust_bridge_codegen generate

# Run Rust tests
cargo test

# Run Flutter app (Linux desktop example)
cd app
flutter run -d linux
```

## Project layout

```
altersend-flutter/
├── app/                    # Flutter UI (thin)
├── rust/
│   ├── altersend_domain/   # Business logic (reducer, join code, page copy)
│   ├── altersend_p2p/      # P2P orchestrator + peeroxide swarm
│   ├── altersend_engine/   # Wires domain + P2P; command API
│   └── altersend_ffi/      # flutter_rust_bridge exports
├── flutter_rust_bridge.yaml
└── Cargo.toml              # Rust workspace
```

## Development

### Rust

```bash
cargo build
cargo test -p altersend_domain
cargo clippy --all-targets
```

### Flutter

```bash
cd app
flutter pub get
flutter analyze
```

## Interoperability note

The original AlterSend uses **Hyperdrive** for encrypted file replication over the same Hyperswarm connections. This repo uses the same **Hyperswarm network** (via [peeroxide](https://github.com/Rightbracket/peeroxide)) and the same **JSON control messages**, but file bytes are not yet replicated via Hyperdrive. Rust ↔ JavaScript transfers will work fully once Hyperdrive replication is integrated (`hypercore` / `hypercore-protocol` crates).

## License

Apache-2.0 — see [LICENSE](LICENSE).
