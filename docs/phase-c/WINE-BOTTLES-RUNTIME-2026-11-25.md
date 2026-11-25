# Wine/Bottles runtime and observer initialization · 2026-11-25

Documentation-only operational clarification for observer C **0.11**. This does
not install, compile or activate an EA, change account permissions, or validate a
live deployment. The software and original experimental tag remain unchanged.
No account identifiers, credentials, private histories or operational logs are
published here.

## Installation is not execution

Keep these claims separate:

1. MQL5 source exists and has a known hash.
2. A matching EX5 was compiled by MetaEditor and placed in the active terminal.
3. The EA is attached with valid local inputs and initialized successfully.
4. A recent stream is being written, accepted by the receiver and reconciled.
5. A real authorized event has reached a persistent record and its note was read
   back at the intended destination.

A source file, an EX5 on disk, a running terminal, an open MCP port or a synthetic
test does not establish the later stages. The public package is not a deployment
installation; it distributes no operational units or account configuration.

## Actual Wine/Bottles dependencies

The observed deployment uses Bottles' Flatpak data layout and a
`kron4ek-wine-11.20-staging-amd64` runner. These are observed runtime details,
not a universal requirement or a claim that other versions are compatible.
Inspect the running process before choosing a path; do not change Bottles or
rename a prefix to match an assumed name.

Typical path relationships, with intentionally generic placeholders:

```text
<Bottles-data>/bottles/<effective-prefix>/
  drive_c/Program Files/<terminal-installation>/
  drive_c/users/<wine-user>/AppData/Roaming/MetaQuotes/Terminal/<instance-id>/
```

The launcher's bottle display name or old command line can differ from the
effective `WINEPREFIX`. Prefer the terminal process's environment, executable,
active files and host/process-root directory identity. A Roaming `origin.txt`
is only a candidate mapping, not proof that its directory is active. Portable
and non-portable installations must be distinguished using current evidence.

The observer uses `FileOpen` **without `FILE_COMMON`**, so its relative spool
belongs in the active terminal's **`MQL5/Files` sandbox**, not automatically in
the working directory, an arbitrary Linux spool or Common/Files. Confirm the
terminal's actual data directory locally. In a verified portable deployment the
sandbox is beneath the installation; otherwise it is beneath the active data
directory. Flatpak visibility, Wine drive mappings, Unix traversal/ownership
and service sandbox access must all agree. Merely checking one directory's
write bit does not prove that MT5 can create and flush its intended stream.

Multiple terminal processes can share a wineserver. Its ownership of listening
sockets does not identify which terminal serves each port. Attribute a port
using the local terminal configuration and an authorized authenticated read of
the terminal workspace/account; do not infer it from a shared wineserver PID.
Do not publish those account reads.

## Interpret the two `reason` namespaces correctly

See the [source](../../ea/phase_c/VidarObserverDemoC.mq5).

| Message | Meaning in C 0.11 |
|---|---|
| `Vidar C blocked; reason=9` | `OnInit` rejected its inputs: `ExpectedLogin <= 0`, empty `ExpectedServer`, or invalid `SourceId`. |
| `Vidar C stopped; reason=8; no deinit I/O.` | MT5 supplied deinitialization reason `REASON_INITFAILED` (8): initialization failed. The suffix describes the deliberate no-write shutdown policy. |

`SourceId` must contain 1–48 ASCII letters, digits, `_` or `-`. The default
`ExpectedLogin=0` and `ExpectedServer=""` are deliberately invalid. Set the
expected identity **locally**, only after verifying the intended demo account;
never hard-code it in published source. These parameters are not credentials.

Internal `Block(9)` runs **before any spool open or write**. These two messages
therefore do not establish a filesystem, bridge, receiver or network failure,
and do not reveal which individual input was wrong. They must not be confused
with internal `Block(8)`, which concerns a failed write/flush/close sequence.
MT5 deinitialization reasons are documented in
[OnDeinit](https://www.mql5.com/en/docs/event_handlers/ondeinit).

After input validation, the existing guards still require a connected **demo**
account and exact expected login/server. Keep those restrictions. Do not remove
them or place/modify/close trades to test initialization. Do not assume that
correcting inputs alone proves the rest of the pipeline.

## Verify each transport boundary separately

C 0.11 has **no HTTP or n8n sender**. Its per-initialization UTF-8 JSONL stream is
named `VidarObserver-<SourceId>-<stream>.jsonl`. Callbacks enqueue; the timer
flushes and snapshots. Deinitialization discards RAM and does not flush, to
avoid mixing identities after an account change. Recover missing history from
an independently verified read-only source.

A separate bridge needs an explicit, reviewed view of that actual sandbox and
a cursor per stream; it must not silently read a different demo's spool. The
receiver, identity configuration, HMAC material, SQLite inbox, provider and
reconciler are further dependencies. An installed unit may be inactive or
blocked by its conditions. Inspect current state and a real receipt at every
stage before claiming continuous capture. Likewise, a CLI reconciliation cycle
does not prove that a publisher exported Markdown or delivered a webhook.

When access to a private data directory is denied, report that narrow access
limit; do not infer an empty spool or failed MT5 write, relax permissions or
activate another demo installation as a substitute. Record last successful
event/ACK/reconciliation/note-read separately from installation state.

## Scope and evidence limits

The input-guard explanation is directly traceable to the published source.
Wine/Bottles layout and shared-wineserver behavior were observed in an
authorized local operational review; the private evidence stays private.
Exact active paths, EX5 provenance, current local input values, sandbox writes
and live end-to-end capture still need their own deployment receipts. No live
initialization or permissions change is claimed by this document.
