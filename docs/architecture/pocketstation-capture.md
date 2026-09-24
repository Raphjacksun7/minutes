# PocketStation capture

Minutes has two opt-in PocketStation integrations behind the
`pocketstation-capture` feature:

- On Windows and Linux, PocketStation records one desktop application without
  a virtual audio device. Minutes retains its existing microphone path.
- On macOS, Minutes retains its Core Audio process-tap backend for system audio
  and uses PocketStation to supervise the microphone in a dual-source recording.

In both cases the microphone and system/application audio remain independent
stems. A microphone failure does not terminate a healthy system-audio stem.

The macOS branch is development-only. It depends on PocketStation Core APIs
that are present in the accepted local Core candidate but are not published in
the `pocketstation` 1.1.10 crate. Local qualification therefore uses a Cargo
patch to that exact Core checkout. Do not describe or release the macOS branch
as an installable feature until those APIs have shipped in a new Core version
and Minutes has updated its dependency to that release.

## Build Minutes

For the published Windows/Linux application-capture path, enable the
`pocketstation-capture` feature:

```bash
cargo build --release -p minutes-cli --features pocketstation-capture
```

That path uses PocketStation 1.1.10 from crates.io and does not require a
PocketStation source checkout. The current macOS microphone-supervision work
cannot be built from crates.io alone for the reason above.

## Windows and Linux: choose an application

Add the following settings to `~/.config/minutes/config.toml`:

```toml
[recording]
capture_backend = "pocketstation"

[recording.sources]
voice = "default"
call = "Zoom"
```

`voice` selects the microphone. `call` selects the application audio and must
contain one of these values:

- an exact application name, such as `"Zoom"`;
- an application ID exposed by the operating system;
- a process ID written as `"pid:1234"`.

PocketStation rejects a name or ID that matches more than one application.
Process IDs are useful for automated tests but change whenever an application
starts. `call = "auto"` is not supported because recording the wrong
application would be difficult to notice.

Start a recording with the normal Minutes command:

```bash
minutes record
```

Minutes writes:

- `meeting.wav`, the mixed recording;
- `meeting.voice.wav`, the microphone;
- `meeting.system.wav`, the selected application.

Minutes writes all three files at 16 kHz and groups both live sources into
100 ms chunks. Each source begins with chunk zero when that source starts.

## macOS: supervise the microphone

Use Minutes' existing Core Audio process tap for system audio and select
`call = "auto"`:

```toml
[recording]
capture_backend = "core-audio-tap"

[recording.sources]
voice = "default"
call = "auto"
```

With `pocketstation-capture` enabled, this exact dual-source configuration uses
PocketStation for the microphone only. The process tap remains a separate
Minutes-owned system-audio backend; PocketStation does not replace it.

The microphone worker observes the opened native format, first and latest
frame activity, and caller-configured signal measurements away from the audio
callback. Minutes distinguishes these conditions:

- the source is waiting for its first frame;
- no frames arrived before the deadline;
- frames stopped arriving;
- frames are arriving but remain exact digital zero;
- the measured signal is below the configured peak/RMS thresholds;
- useful signal was observed;
- samples were non-finite; or
- the source failed.

For a recoverable microphone failure, Minutes requests one bounded reopen of
the exact selected device. If that reopen fails, or if the reopened device is
still unhealthy after the observation window, Minutes resolves an explicit
fallback device ID and asks PocketStation to replace the source. It uses the
current default when that is a different physical device; otherwise it prefers
an available built-in/internal input before another alternative. One fallback
is attempted. If it also remains unhealthy, Minutes continues in system-only
degraded mode instead of looping forever. A replacement carries a new source
generation and discontinuity instead of pretending that two physical devices
are one uninterrupted source. While the microphone is unavailable, the system
stem continues and missing voice slots are represented as silence.

This is host policy, not automatic Core policy: PocketStation supplies the
measurements, explicit reopen/replacement operations, lineage, and bounded
delivery; Minutes chooses when to retry or replace the source.

## Timing and evidence limits

The Windows/Linux application-capture path still does not measure a common
start time between the microphone and application capture, so a delayed source
is not backfilled from the other source's start time.

The macOS adapter persists PocketStation microphone lineage beside the voice
stem as `<recording>.voice.lineage.jsonl`, including source, stem and clock IDs,
source sequence bounds, missing-sequence counts, inserted-silence samples,
source timestamps, source generation, and discontinuity and permission epochs.
When sequence and timestamp evidence agree that canonical 10 ms frames are
missing, Minutes inserts the corresponding 16 kHz silence instead of
compressing the microphone timeline. Inconsistent or unbounded gaps fail the
microphone worker and enter the same bounded host recovery path.

The adapter does not yet correlate the PocketStation microphone clock with the
independent Core Audio process-tap clock. The mixer still aligns the two inputs
through Minutes' chunk positions, so this branch must not claim a measured
common microphone/system timeline or bounded cross-source drift.

Unit and synthetic integration tests cover classification, bounded queues,
cancel-before-join shutdown, source replacement lineage, and survival of the
system stem while the microphone is unavailable. No physical Logi HFP headset
or Teams call has been tested through this branch yet. Until that hardware
matrix passes, this is not proof that Minutes issue #1057 is fixed.

## Failure behavior

PocketStation delivers 10 ms audio frames to a dedicated Minutes worker. The
worker converts canonical 48 kHz audio to the 16 kHz format used by Minutes and
groups it into 100 ms chunks. PocketStation opens supported native microphone
formats and performs conversion/resampling before delivering those canonical
frames; Minutes exposes the actual opened format in its observations.

Each PocketStation path uses a bounded 64-chunk output queue, which is 6.4
seconds at this format. If the recording loop cannot keep up, new chunks are
dropped and the total is written to the log. A stalled consumer cannot block
the capture callback. Stopping a recording cancels the polled-audio Session
before Minutes waits for the worker, so finalization does not drain pending
delivery. Worker shutdown is bounded to two seconds; a worker that does not
confirm cancellation is detached and reported instead of blocking recording
finalization indefinitely.

Application capture does not bypass microphone permissions, operating-system
audio permissions, or Minutes' recording consent settings.
