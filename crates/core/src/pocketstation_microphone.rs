use crate::error::CaptureError;
use crate::streaming::{AudioChunk, AudioChunkLineage, ChunkAccumulator, SourceRole};
use pocketstation::{
    AudioFrameDuration, CaptureNativeFormat, DeviceId, DeviceSelector, Session, SessionEventKind,
    SessionSourceActivityObservations, SessionSourceActivityPolicy, SessionSourceActivityState,
    SessionSourceReplacement, SessionSourceSignalEvaluation, SessionSourceSignalObservations,
    SessionSourceSignalPolicy, SessionSourceSignalState, Source, SourceKind, StemId,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const AUDIO_POLL_TIMEOUT: Duration = Duration::from_millis(50);
const CONTROL_TIMEOUT: Duration = Duration::from_millis(1_500);
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const FIRST_FRAME_TIMEOUT: Duration = Duration::from_secs(2);
const STALL_TIMEOUT: Duration = Duration::from_secs(2);
const EXACT_ZERO_TIMEOUT: Duration = Duration::from_millis(1_500);
const MINIMUM_PEAK_DBFS: f64 = -45.0;
const MINIMUM_RMS_DBFS: f64 = -55.0;
// #1057's failed Logi HFP stem was not bit-exact zero: its measured peak was
// -78.3 dBFS and its mean level was -91 dBFS. Keep this observation threshold
// well below ordinary quiet speech, and require it to persist before warning
// the user. Amplitude alone never authorizes source recovery.
const UNUSABLE_LOW_SIGNAL_MAXIMUM_PEAK_DBFS: f64 = -70.0;
const UNUSABLE_LOW_SIGNAL_MAXIMUM_RMS_DBFS: f64 = -80.0;
const UNUSABLE_LOW_SIGNAL_TIMEOUT: Duration = Duration::from_millis(1_500);
const OUTPUT_QUEUE_CAPACITY_CHUNKS: usize = 64;
const CONTROL_QUEUE_CAPACITY: usize = 4;
const POCKETSTATION_SAMPLE_RATE_HZ: u32 = 48_000;
const MINUTES_SAMPLE_RATE_HZ: u32 = 16_000;
const MAX_SEQUENCE_GAP_DURATION: Duration = Duration::from_secs(2);
const MAX_TIMESTAMP_JITTER: Duration = Duration::from_millis(2);
const SOURCE_FRAME_DURATION_NS: u64 = 10_000_000;
const SOURCE_FRAME_SAMPLES: usize =
    (MINUTES_SAMPLE_RATE_HZ as usize * SOURCE_FRAME_DURATION_NS as usize) / 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MicrophoneSignalState {
    AwaitingFirstFrame,
    Active,
    NoFrames,
    Stalled,
    ExactDigitalZeroPending,
    DigitallySilent,
    BelowThresholds,
    SustainedLowSignal,
    SignalObserved,
    NonFiniteSamples,
    SourceFailed,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MicrophoneObservations {
    pub(crate) state: MicrophoneSignalState,
    pub(crate) native_format: Option<CaptureNativeFormat>,
    pub(crate) activity: Option<SessionSourceActivityObservations>,
    pub(crate) signal: Option<SessionSourceSignalObservations>,
    pub(crate) source_generation: u32,
    pub(crate) discontinuity_epoch: u64,
}

impl Default for MicrophoneObservations {
    fn default() -> Self {
        Self {
            state: MicrophoneSignalState::AwaitingFirstFrame,
            native_format: None,
            activity: None,
            signal: None,
            source_generation: 1,
            discontinuity_epoch: 0,
        }
    }
}

impl MicrophoneObservations {
    pub(crate) fn requires_recovery(self) -> bool {
        matches!(
            self.state,
            MicrophoneSignalState::NoFrames
                | MicrophoneSignalState::Stalled
                | MicrophoneSignalState::DigitallySilent
                | MicrophoneSignalState::NonFiniteSamples
                | MicrophoneSignalState::SourceFailed
        )
    }

    pub(crate) fn confirms_recovery(self) -> bool {
        self.state == MicrophoneSignalState::SignalObserved
    }
}

#[derive(Debug, Default)]
struct LowSignalTracker {
    started_at_ns: Option<u64>,
    source_generation: u32,
    discontinuity_epoch: u64,
}

impl LowSignalTracker {
    fn observe(
        &mut self,
        observed_at_ns: u64,
        source_generation: u32,
        discontinuity_epoch: u64,
        peak_dbfs: Option<f64>,
        rms_dbfs: Option<f64>,
    ) -> bool {
        let source_changed = self.source_generation != source_generation
            || self.discontinuity_epoch != discontinuity_epoch;
        let unusably_low = peak_dbfs.is_some_and(|peak| {
            peak <= UNUSABLE_LOW_SIGNAL_MAXIMUM_PEAK_DBFS
                && rms_dbfs.is_some_and(|rms| rms <= UNUSABLE_LOW_SIGNAL_MAXIMUM_RMS_DBFS)
        });

        if source_changed || !unusably_low {
            self.started_at_ns = None;
            self.source_generation = source_generation;
            self.discontinuity_epoch = discontinuity_epoch;
        }
        if !unusably_low {
            return false;
        }

        let started_at_ns = *self.started_at_ns.get_or_insert(observed_at_ns);
        observed_at_ns.saturating_sub(started_at_ns)
            >= UNUSABLE_LOW_SIGNAL_TIMEOUT.as_nanos() as u64
    }
}

#[derive(Debug, Clone)]
struct MicrophoneSelection {
    selector: DeviceSelector,
    device_id: String,
    display_name: String,
}

impl MicrophoneSelection {
    fn resolve(
        device_override: Option<&str>,
        resolved_default_name: &str,
    ) -> Result<Self, CaptureError> {
        use cpal::traits::{DeviceTrait, HostTrait};

        let explicit_request = device_override
            .map(str::trim)
            .filter(|value| !value.is_empty() && !value.eq_ignore_ascii_case("default"));
        let (requested, exact_device_id) = if let Some(requested) = explicit_request {
            (requested.to_owned(), None)
        } else {
            let default = cpal::default_host().default_input_device().ok_or_else(|| {
                capture_error(
                    "select PocketStation microphone",
                    "no default input device is available",
                )
            })?;
            let device_id = default.id().map_err(|error| {
                capture_error(
                    "select PocketStation microphone",
                    format!("read default input device identity: {error}"),
                )
            })?;
            (
                resolved_default_name.to_owned(),
                Some(device_id.to_string()),
            )
        };

        select_discovered_microphone(
            discovered_microphones(),
            &requested,
            exact_device_id.as_deref(),
        )
    }

    fn recovery_fallback(
        current_device_id: Option<&str>,
        resolved_default_name: &str,
    ) -> Result<Self, CaptureError> {
        let default = Self::resolve(None, resolved_default_name).ok();
        choose_recovery_fallback(current_device_id, default).ok_or_else(|| {
            capture_error(
                "select PocketStation microphone fallback",
                "the system default input still resolves to the failed microphone; choose another microphone to continue voice capture",
            )
        })
    }
}

fn select_discovered_microphone(
    sources: Vec<MicrophoneSelection>,
    requested: &str,
    exact_device_id: Option<&str>,
) -> Result<MicrophoneSelection, CaptureError> {
    let mut matching = sources.into_iter().filter(|source| {
        exact_device_id.map_or_else(
            || source.display_name.eq_ignore_ascii_case(requested) || source.device_id == requested,
            |device_id| source.device_id == device_id,
        )
    });
    let selected = matching.next().ok_or_else(|| {
        capture_error(
            "select PocketStation microphone",
            format!("no input device matches '{requested}'"),
        )
    })?;
    if matching.next().is_some() {
        return Err(capture_error(
            "select PocketStation microphone",
            format!("more than one input device matches '{requested}'"),
        ));
    }
    Ok(selected)
}

fn discovered_microphones() -> Vec<MicrophoneSelection> {
    pocketstation::discover_sources()
        .into_iter()
        .filter(|source| source.stable_id.kind == SourceKind::InputDevice)
        .filter_map(|source| {
            let device_id = source.device_uid?;
            Some(MicrophoneSelection {
                selector: DeviceSelector::id(DeviceId::new(device_id.clone())),
                device_id,
                display_name: source.name,
            })
        })
        .collect()
}

fn choose_recovery_fallback(
    current_device_id: Option<&str>,
    default: Option<MicrophoneSelection>,
) -> Option<MicrophoneSelection> {
    let current_device_id = current_device_id?;
    default.filter(|default| default.device_id != current_device_id)
}

#[derive(Debug, Clone, Copy)]
struct SignalWindowContinuity {
    source_generation: u32,
    discontinuity_epoch: u64,
    samples_observed_total: u64,
}

impl From<SessionSourceSignalObservations> for SignalWindowContinuity {
    fn from(observations: SessionSourceSignalObservations) -> Self {
        Self {
            source_generation: observations.window_source_generation,
            discontinuity_epoch: observations.window_discontinuity_epoch,
            samples_observed_total: observations.samples_observed_total,
        }
    }
}

#[derive(Debug, Default)]
struct ObservationContinuityTracker {
    continuity: Option<(u32, u64)>,
    started_at_ns: u64,
    activity_frames_baseline: u64,
    signal_samples_baseline: u64,
}

impl ObservationContinuityTracker {
    fn project_activity(
        &mut self,
        activity: Option<SessionSourceActivityObservations>,
        signal: Option<SignalWindowContinuity>,
        source_generation: u32,
        discontinuity_epoch: u64,
    ) -> (Option<SessionSourceActivityObservations>, bool) {
        let continuity = (source_generation, discontinuity_epoch);
        if self.continuity != Some(continuity) {
            let first_attachment = self.continuity.is_none();
            self.continuity = Some(continuity);
            self.started_at_ns = activity.map_or(0, |entry| {
                if first_attachment {
                    entry.session_started_at_ns
                } else {
                    entry.observed_at_ns
                }
            });
            self.activity_frames_baseline = if first_attachment {
                0
            } else {
                activity.map_or(0, |entry| entry.frames_received_total)
            };
            self.signal_samples_baseline = if first_attachment {
                0
            } else {
                signal.map_or(0, |entry| entry.samples_observed_total)
            };
        }
        if self.started_at_ns == 0 {
            self.started_at_ns = activity.map_or(0, |entry| entry.observed_at_ns);
        }

        let current_signal_observed = signal.is_some_and(|entry| {
            entry.source_generation == source_generation
                && entry.discontinuity_epoch == discontinuity_epoch
                && entry.samples_observed_total > self.signal_samples_baseline
        });
        let projected_activity = activity.map(|entry| {
            let frames_received_total = entry
                .frames_received_total
                .saturating_sub(self.activity_frames_baseline);
            let current_frame_observed = current_signal_observed && frames_received_total > 0;
            let latest_frame_received_at_ns = if current_frame_observed {
                entry.latest_frame_received_at_ns
            } else {
                None
            };
            SessionSourceActivityObservations {
                session_started_at_ns: self.started_at_ns,
                observed_at_ns: entry.observed_at_ns,
                first_frame_received_at_ns: latest_frame_received_at_ns,
                latest_frame_received_at_ns,
                frames_received_total: if current_frame_observed {
                    frames_received_total
                } else {
                    0
                },
            }
        });
        (projected_activity, current_signal_observed)
    }
}

#[derive(Clone)]
struct MicrophoneDiagnosticIdentity {
    device_id: String,
    display_name: String,
}

impl From<&MicrophoneSelection> for MicrophoneDiagnosticIdentity {
    fn from(selection: &MicrophoneSelection) -> Self {
        Self {
            device_id: selection.device_id.clone(),
            display_name: selection.display_name.clone(),
        }
    }
}

enum MicrophoneControl {
    Reopen {
        selector: DeviceSelector,
        identity: MicrophoneDiagnosticIdentity,
        response: crossbeam_channel::Sender<Result<SessionSourceReplacement, String>>,
    },
    Replace {
        selector: DeviceSelector,
        identity: MicrophoneDiagnosticIdentity,
        response: crossbeam_channel::Sender<Result<SessionSourceReplacement, String>>,
    },
}

pub(crate) struct PocketStationMicrophoneStream {
    stop: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    cancel_completed: Arc<AtomicBool>,
    cancel_succeeded: Arc<AtomicBool>,
    worker_finished: crossbeam_channel::Receiver<()>,
    dropped_chunks_total: Arc<AtomicU64>,
    observations: Arc<Mutex<MicrophoneObservations>>,
    control: crossbeam_channel::Sender<MicrophoneControl>,
    worker: Option<JoinHandle<()>>,
    selector: DeviceSelector,
    device_id: String,
    pub(crate) receiver: crossbeam_channel::Receiver<AudioChunk>,
    pub(crate) device_name: String,
}

impl PocketStationMicrophoneStream {
    pub(crate) fn start(
        device_override: Option<&str>,
        resolved_default_name: &str,
    ) -> Result<Self, CaptureError> {
        let selection = MicrophoneSelection::resolve(device_override, resolved_default_name)?;
        Self::start_selection(selection)
    }

    pub(crate) fn start_fallback(
        previous_device_id: Option<&str>,
        resolved_default_name: &str,
    ) -> Result<Self, CaptureError> {
        let fallback =
            MicrophoneSelection::recovery_fallback(previous_device_id, resolved_default_name)?;
        Self::start_selection(fallback)
    }

    fn start_selection(selection: MicrophoneSelection) -> Result<Self, CaptureError> {
        let diagnostic_identity = MicrophoneDiagnosticIdentity::from(&selection);
        let session = Session::builder()
            .audio_frame_duration(AudioFrameDuration::Ms10)
            .build();
        let microphone = session
            .capture(Source::microphone(selection.selector.clone()))
            .map_err(|error| capture_error("select PocketStation microphone", error))?;
        let stem_id = microphone.id();
        let output = session
            .polled_audio()
            .map_err(|error| capture_error("open PocketStation microphone output", error))?;
        microphone
            .send(output)
            .map_err(|error| capture_error("connect PocketStation microphone", error))?;
        let running = session
            .start()
            .map_err(|error| capture_error("start PocketStation microphone", error))?;

        let (audio_sender, receiver) = crossbeam_channel::bounded(OUTPUT_QUEUE_CAPACITY_CHUNKS);
        let (control, control_receiver) = crossbeam_channel::bounded(CONTROL_QUEUE_CAPACITY);
        let stop = Arc::new(AtomicBool::new(false));
        let failed = Arc::new(AtomicBool::new(false));
        let cancel_completed = Arc::new(AtomicBool::new(false));
        let cancel_succeeded = Arc::new(AtomicBool::new(false));
        let source_failed = Arc::new(AtomicBool::new(false));
        let dropped_chunks_total = Arc::new(AtomicU64::new(0));
        let observations = Arc::new(Mutex::new(MicrophoneObservations::default()));
        let (worker_finished_sender, worker_finished) = crossbeam_channel::bounded(1);

        let worker = Self::spawn(
            running,
            stem_id,
            audio_sender,
            control_receiver,
            Arc::clone(&stop),
            Arc::clone(&failed),
            Arc::clone(&cancel_completed),
            Arc::clone(&cancel_succeeded),
            Arc::clone(&source_failed),
            Arc::clone(&dropped_chunks_total),
            Arc::clone(&observations),
            diagnostic_identity,
            worker_finished_sender,
        )?;

        Ok(Self {
            stop,
            failed,
            cancel_completed,
            cancel_succeeded,
            worker_finished,
            dropped_chunks_total,
            observations,
            control,
            worker: Some(worker),
            selector: selection.selector,
            device_id: selection.device_id,
            receiver,
            device_name: selection.display_name,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn(
        mut running: pocketstation::RunningSession,
        stem_id: StemId,
        sink: crossbeam_channel::Sender<AudioChunk>,
        control: crossbeam_channel::Receiver<MicrophoneControl>,
        stop: Arc<AtomicBool>,
        failed: Arc<AtomicBool>,
        cancel_completed: Arc<AtomicBool>,
        cancel_succeeded: Arc<AtomicBool>,
        source_failed: Arc<AtomicBool>,
        dropped_chunks_total: Arc<AtomicU64>,
        observations: Arc<Mutex<MicrophoneObservations>>,
        diagnostic_identity: MicrophoneDiagnosticIdentity,
        worker_finished: crossbeam_channel::Sender<()>,
    ) -> Result<JoinHandle<()>, CaptureError> {
        std::thread::Builder::new()
            .name("minutes-pocketstation-microphone".into())
            .spawn(move || {
                let activity_policy = SessionSourceActivityPolicy::new(
                    FIRST_FRAME_TIMEOUT,
                    STALL_TIMEOUT,
                )
                .expect("fixed microphone activity policy must be valid");
                let signal_policy = SessionSourceSignalPolicy::new(
                    MINIMUM_PEAK_DBFS,
                    MINIMUM_RMS_DBFS,
                    EXACT_ZERO_TIMEOUT,
                )
                .expect("fixed microphone signal policy must be valid");
                let mut writer = MicrophoneAudioChunkWriter::default();
                let mut low_signal_tracker = LowSignalTracker::default();
                let mut observation_continuity = ObservationContinuityTracker::default();
                let mut diagnostic_identity = diagnostic_identity;
                let mut logged_native_format_attachment = None;
                let mut pending_format_continuity = None;

                while !stop.load(Ordering::Relaxed) {
                    while let Ok(command) = control.try_recv() {
                        source_failed.store(false, Ordering::Relaxed);
                        let (result, identity, response) = match command {
                            MicrophoneControl::Reopen {
                                selector,
                                identity,
                                response,
                            } => (
                                running
                                    .reopen_microphone_source(stem_id, selector)
                                    .map_err(|error| error.to_string()),
                                identity,
                                response,
                            ),
                            MicrophoneControl::Replace {
                                selector,
                                identity,
                                response,
                            } => (
                                running
                                    .replace_microphone_source(stem_id, selector)
                                    .map_err(|error| error.to_string()),
                                identity,
                                response,
                            ),
                        };
                        if let Ok(replacement) = &result {
                            diagnostic_identity = identity;
                            logged_native_format_attachment = None;
                            pending_format_continuity = Some((
                                replacement.source_generation,
                                replacement.discontinuity_epoch,
                            ));
                        }
                        writer.reset_for_discontinuity();
                        let _ = response.try_send(result);
                    }

                    match running.wait_audio(AUDIO_POLL_TIMEOUT) {
                        Ok(Some(batch)) => {
                            for frame_index in 0..batch.len() {
                                let Some(frame) = batch.frame(frame_index) else {
                                    eprintln!(
                                        "[minutes] PocketStation microphone returned an invalid frame lease at batch index {frame_index}"
                                    );
                                    failed.store(true, Ordering::Relaxed);
                                    break;
                                };
                                if frame.lineage().stem_id() != stem_id {
                                    continue;
                                }
                                if let Err(error) = writer.write_frame(
                                    frame,
                                    &sink,
                                    &dropped_chunks_total,
                                ) {
                                    eprintln!(
                                        "[minutes] PocketStation microphone frame conversion failed: {error}"
                                    );
                                    tracing::error!(error = %error, "PocketStation microphone stopped");
                                    failed.store(true, Ordering::Relaxed);
                                    break;
                                }
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            eprintln!(
                                "[minutes] PocketStation microphone polling failed: {error}"
                            );
                            tracing::error!(error = %error, "PocketStation microphone polling failed");
                            failed.store(true, Ordering::Relaxed);
                        }
                    }

                    while let pocketstation::SessionEventReceive::Event(event) =
                        running.try_recv_event()
                    {
                        match event.kind() {
                            SessionEventKind::Source(failure) => {
                                eprintln!(
                                    "[minutes] PocketStation microphone source failed: {:?}",
                                    failure.event()
                                );
                                source_failed.store(true, Ordering::Relaxed);
                            }
                            SessionEventKind::Endpoint(_)
                            | SessionEventKind::Rollback(_)
                            | SessionEventKind::Finalization(_)
                            | SessionEventKind::Lifecycle(
                                pocketstation::SessionLifecycleState::Failed,
                            ) => {
                                eprintln!(
                                    "[minutes] PocketStation microphone session failed: {:?}",
                                    event.kind()
                                );
                                failed.store(true, Ordering::Relaxed);
                            }
                            SessionEventKind::Terminal(outcome)
                                if outcome.state()
                                    == pocketstation::SessionTerminalState::Failed =>
                            {
                                eprintln!(
                                    "[minutes] PocketStation microphone terminal failure: {:?}",
                                    outcome
                                );
                                failed.store(true, Ordering::Relaxed);
                            }
                            _ => {}
                        }
                    }

                    if let Some(current_observations) = update_observations(
                        &running,
                        stem_id,
                        activity_policy,
                        signal_policy,
                        &mut low_signal_tracker,
                        &mut observation_continuity,
                        source_failed.load(Ordering::Relaxed),
                        &observations,
                    ) {
                        if report_opened_native_format_once(
                            current_observations,
                            &diagnostic_identity,
                            pending_format_continuity,
                            &mut logged_native_format_attachment,
                        ) {
                            pending_format_continuity = None;
                        }
                    }
                    if failed.load(Ordering::Relaxed) {
                        break;
                    }
                }

                let cancellation_succeeded = running.cancel().is_success();
                cancel_succeeded.store(cancellation_succeeded, Ordering::Release);
                cancel_completed.store(true, Ordering::Release);
                if !cancellation_succeeded {
                    eprintln!("[minutes] PocketStation microphone cancellation failed");
                    failed.store(true, Ordering::Relaxed);
                }
                let _ = worker_finished.try_send(());
            })
            .map_err(|error| capture_error("start PocketStation microphone worker", error))
    }

    pub(crate) fn has_error(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    pub(crate) fn observations(&self) -> MicrophoneObservations {
        self.observations
            .lock()
            .map(|observations| *observations)
            .unwrap_or(MicrophoneObservations {
                state: MicrophoneSignalState::SourceFailed,
                ..MicrophoneObservations::default()
            })
    }

    pub(crate) fn reopen_exact(&self) -> Result<SessionSourceReplacement, CaptureError> {
        self.request_control(|response| MicrophoneControl::Reopen {
            selector: self.selector.clone(),
            identity: MicrophoneDiagnosticIdentity {
                device_id: self.device_id.clone(),
                display_name: self.device_name.clone(),
            },
            response,
        })
    }

    pub(crate) fn replace_with_fallback(
        &mut self,
        resolved_default_name: String,
    ) -> Result<SessionSourceReplacement, CaptureError> {
        let selection =
            MicrophoneSelection::recovery_fallback(Some(&self.device_id), &resolved_default_name)?;
        let selector = selection.selector.clone();
        let result = self.request_control(|response| MicrophoneControl::Replace {
            selector: selector.clone(),
            identity: MicrophoneDiagnosticIdentity::from(&selection),
            response,
        })?;
        self.selector = selector;
        self.device_id = selection.device_id;
        self.device_name = selection.display_name;
        Ok(result)
    }

    fn request_control(
        &self,
        command: impl FnOnce(
            crossbeam_channel::Sender<Result<SessionSourceReplacement, String>>,
        ) -> MicrophoneControl,
    ) -> Result<SessionSourceReplacement, CaptureError> {
        let (response, receiver) = crossbeam_channel::bounded(1);
        self.control
            .send_timeout(command(response), CONTROL_TIMEOUT)
            .map_err(|error| capture_error("send PocketStation microphone control", error))?;
        receiver
            .recv_timeout(CONTROL_TIMEOUT)
            .map_err(|error| capture_error("wait for PocketStation microphone control", error))?
            .map_err(|error| capture_error("apply PocketStation microphone control", error))
    }
}

impl Drop for PocketStationMicrophoneStream {
    fn drop(&mut self) {
        if !stop_cancel_and_join(
            &self.stop,
            self.worker.take(),
            &self.cancel_completed,
            &self.cancel_succeeded,
            &self.worker_finished,
            WORKER_SHUTDOWN_TIMEOUT,
        ) {
            self.failed.store(true, Ordering::Relaxed);
            tracing::error!("PocketStation microphone worker did not cancel cleanly before join");
        }
        let dropped_chunks_total = self.dropped_chunks_total.load(Ordering::Relaxed);
        if dropped_chunks_total > 0 {
            tracing::warn!(
                dropped_chunks_total,
                "Minutes dropped PocketStation microphone audio because its queue was full"
            );
        }
    }
}

fn stop_cancel_and_join(
    stop: &AtomicBool,
    worker: Option<JoinHandle<()>>,
    cancel_completed: &AtomicBool,
    cancel_succeeded: &AtomicBool,
    worker_finished: &crossbeam_channel::Receiver<()>,
    timeout: Duration,
) -> bool {
    stop.store(true, Ordering::Release);
    let Some(worker) = worker else {
        return cancel_completed.load(Ordering::Acquire)
            && cancel_succeeded.load(Ordering::Acquire);
    };
    match worker_finished.recv_timeout(timeout) {
        Ok(()) => {
            worker.join().is_ok()
                && cancel_completed.load(Ordering::Acquire)
                && cancel_succeeded.load(Ordering::Acquire)
        }
        Err(_) => {
            drop(worker);
            false
        }
    }
}

fn update_observations(
    running: &pocketstation::RunningSession,
    stem_id: StemId,
    activity_policy: SessionSourceActivityPolicy,
    signal_policy: SessionSourceSignalPolicy,
    low_signal_tracker: &mut LowSignalTracker,
    observation_continuity: &mut ObservationContinuityTracker,
    source_failed: bool,
    observations: &Mutex<MicrophoneObservations>,
) -> Option<MicrophoneObservations> {
    let Ok(snapshot) = running.metrics_snapshot() else {
        return None;
    };
    let Some(source_index) = (0..snapshot.source_count()).find(|&index| {
        snapshot
            .source(index)
            .is_some_and(|source| source.stem_id == stem_id)
    }) else {
        return None;
    };
    let raw_activity = snapshot.source_activity(source_index).copied();
    let raw_signal = snapshot.source_signal(source_index).copied();
    let replacement = snapshot.source_replacement(source_index).copied();
    let native_format = snapshot
        .source_native_format(source_index)
        .and_then(|entry| entry.opened_native_format);
    let source_generation = replacement.map_or(1, |entry| entry.source_generation);
    let discontinuity_epoch = replacement.map_or(0, |entry| entry.discontinuity_epoch);
    let (activity, current_signal_observed) = observation_continuity.project_activity(
        raw_activity,
        raw_signal.map(SignalWindowContinuity::from),
        source_generation,
        discontinuity_epoch,
    );
    let signal = current_signal_observed.then_some(raw_signal).flatten();
    let state = evaluate_observations(
        activity,
        signal,
        activity_policy,
        signal_policy,
        low_signal_tracker,
        source_generation,
        discontinuity_epoch,
        source_failed,
    );
    let observation_snapshot = MicrophoneObservations {
        state,
        native_format,
        activity,
        signal,
        source_generation,
        discontinuity_epoch,
    };
    if let Ok(mut current) = observations.lock() {
        *current = observation_snapshot;
    }
    Some(observation_snapshot)
}

fn evaluate_observations(
    activity: Option<SessionSourceActivityObservations>,
    signal: Option<SessionSourceSignalObservations>,
    activity_policy: SessionSourceActivityPolicy,
    signal_policy: SessionSourceSignalPolicy,
    low_signal_tracker: &mut LowSignalTracker,
    source_generation: u32,
    discontinuity_epoch: u64,
    source_failed: bool,
) -> MicrophoneSignalState {
    let activity_state = activity.map(|activity| activity.evaluate(activity_policy).state);
    let signal_evaluation = signal.map(|signal| signal.evaluate(signal_policy));
    evaluate_state(
        activity_state,
        signal_evaluation,
        signal.map_or_else(
            || activity.map_or(0, |entry| entry.observed_at_ns),
            |entry| entry.observed_at_ns,
        ),
        low_signal_tracker,
        source_generation,
        discontinuity_epoch,
        source_failed,
    )
}

fn evaluate_state(
    activity_state: Option<SessionSourceActivityState>,
    signal_evaluation: Option<SessionSourceSignalEvaluation>,
    observed_at_ns: u64,
    low_signal_tracker: &mut LowSignalTracker,
    source_generation: u32,
    discontinuity_epoch: u64,
    source_failed: bool,
) -> MicrophoneSignalState {
    if source_failed {
        low_signal_tracker.started_at_ns = None;
        return MicrophoneSignalState::SourceFailed;
    }
    let sustained_low_signal = signal_evaluation.is_some_and(|evaluation| {
        evaluation.state == SessionSourceSignalState::BelowCallerThresholds
            && low_signal_tracker.observe(
                observed_at_ns,
                source_generation,
                discontinuity_epoch,
                evaluation.peak_dbfs,
                evaluation.rms_dbfs,
            )
    });
    if !matches!(
        signal_evaluation,
        Some(evaluation) if evaluation.state == SessionSourceSignalState::BelowCallerThresholds
    ) {
        low_signal_tracker.started_at_ns = None;
    }
    classify_evaluations(
        activity_state,
        signal_evaluation.map(|evaluation| evaluation.state),
        sustained_low_signal,
    )
}

fn classify_evaluations(
    activity: Option<SessionSourceActivityState>,
    signal: Option<SessionSourceSignalState>,
    sustained_low_signal: bool,
) -> MicrophoneSignalState {
    match activity {
        None | Some(SessionSourceActivityState::AwaitingFirstFrame) => {
            return MicrophoneSignalState::AwaitingFirstFrame;
        }
        Some(SessionSourceActivityState::FirstFrameTimedOut) => {
            return MicrophoneSignalState::NoFrames;
        }
        Some(SessionSourceActivityState::Stalled) => {
            return MicrophoneSignalState::Stalled;
        }
        Some(SessionSourceActivityState::Active) => {}
    }
    match signal {
        None | Some(SessionSourceSignalState::NoSamplesObserved) => MicrophoneSignalState::Active,
        Some(SessionSourceSignalState::ExactDigitalZeroPending) => {
            MicrophoneSignalState::ExactDigitalZeroPending
        }
        Some(SessionSourceSignalState::SustainedExactDigitalZero) => {
            MicrophoneSignalState::DigitallySilent
        }
        Some(SessionSourceSignalState::BelowCallerThresholds) if sustained_low_signal => {
            MicrophoneSignalState::SustainedLowSignal
        }
        Some(SessionSourceSignalState::BelowCallerThresholds) => {
            MicrophoneSignalState::BelowThresholds
        }
        Some(SessionSourceSignalState::MeetsCallerThresholds) => {
            MicrophoneSignalState::SignalObserved
        }
        Some(SessionSourceSignalState::NonFiniteSamplesObserved) => {
            MicrophoneSignalState::NonFiniteSamples
        }
    }
}

fn report_opened_native_format_once(
    observations: MicrophoneObservations,
    identity: &MicrophoneDiagnosticIdentity,
    minimum_continuity: Option<(u32, u64)>,
    logged: &mut Option<(u32, String)>,
) -> bool {
    if minimum_continuity.is_some_and(|minimum| {
        (
            observations.source_generation,
            observations.discontinuity_epoch,
        ) < minimum
    }) {
        return false;
    }
    let Some(native_format) = observations.native_format else {
        return false;
    };
    let key = opened_native_format_diagnostic_key(observations, identity);
    if logged.as_ref() == Some(&key) {
        return false;
    }

    let entry = opened_native_format_log_entry(
        identity,
        native_format,
        observations.source_generation,
        observations.discontinuity_epoch,
    );
    eprintln!(
        "[minutes] PocketStation microphone opened: {} Hz, {} channel(s), {:?}",
        native_format.sample_rate_hz,
        native_format.channel_count,
        native_format.sample_representation
    );
    if let Err(error) = crate::logging::append_log(&entry) {
        tracing::warn!(%error, "failed to persist PocketStation microphone format diagnostic");
    }
    *logged = Some(key);
    true
}

fn opened_native_format_diagnostic_key(
    observations: MicrophoneObservations,
    identity: &MicrophoneDiagnosticIdentity,
) -> (u32, String) {
    (observations.source_generation, identity.device_id.clone())
}

fn opened_native_format_log_entry(
    identity: &MicrophoneDiagnosticIdentity,
    native_format: CaptureNativeFormat,
    source_generation: u32,
    discontinuity_epoch: u64,
) -> serde_json::Value {
    serde_json::json!({
        "ts": chrono::Local::now().to_rfc3339(),
        "level": "info",
        "step": "pocketstation_microphone_opened_format",
        "device_id": identity.device_id,
        "device_name": identity.display_name,
        "sample_rate_hz": native_format.sample_rate_hz,
        "channel_count": native_format.channel_count,
        "sample_representation": format!("{:?}", native_format.sample_representation),
        "source_generation": source_generation,
        "discontinuity_epoch": discontinuity_epoch,
        "message": "PocketStation microphone opened a native input format",
    })
}

#[derive(Default)]
struct MicrophoneAudioChunkWriter {
    downsample_phase_samples: usize,
    resampled_samples: Vec<f32>,
    chunks: ChunkAccumulator,
    pending_lineage: Option<AudioChunkLineage>,
    latest_source_frame: Option<AudioChunkLineage>,
}

impl MicrophoneAudioChunkWriter {
    fn reset_for_discontinuity(&mut self) {
        self.downsample_phase_samples = 0;
        self.resampled_samples.clear();
        self.chunks.clear();
        self.pending_lineage = None;
        self.latest_source_frame = None;
    }

    fn write_frame(
        &mut self,
        frame: pocketstation::PolledAudioFrame<'_>,
        sink: &crossbeam_channel::Sender<AudioChunk>,
        dropped_chunks_total: &AtomicU64,
    ) -> Result<(), CaptureError> {
        if frame.sample_rate_hz() != POCKETSTATION_SAMPLE_RATE_HZ {
            return Err(capture_error(
                "read PocketStation microphone",
                format!(
                    "expected {POCKETSTATION_SAMPLE_RATE_HZ} Hz canonical audio, received {} Hz",
                    frame.sample_rate_hz()
                ),
            ));
        }
        let channel_count = usize::from(frame.channels());
        if channel_count == 0 || !frame.samples().len().is_multiple_of(channel_count) {
            return Err(capture_error(
                "read PocketStation microphone",
                "received an invalid channel layout",
            ));
        }

        let lineage = frame.lineage();
        let frame_lineage = AudioChunkLineage {
            session_id: lineage.session_id().get(),
            source_id: lineage.source_id().get(),
            stem_id: lineage.stem_id().get(),
            clock_id: lineage.clock_id().get(),
            first_sequence_number: lineage.sequence_number(),
            last_sequence_number: lineage.sequence_number(),
            missing_sequence_count: 0,
            inserted_silence_samples: 0,
            timestamp_start_ns: lineage.timestamp_start_ns(),
            duration_ns: lineage.duration_ns(),
            source_generation: lineage.source_generation(),
            discontinuity_epoch: lineage.discontinuity_epoch(),
            permission_epoch: lineage.permission_epoch(),
            observed_at_ns: frame.route_received_at_ns(),
            polled_at_ns: frame.polled_at_ns(),
        };
        self.resampled_samples.clear();
        let downsample_ratio = (POCKETSTATION_SAMPLE_RATE_HZ / MINUTES_SAMPLE_RATE_HZ) as usize;
        for samples in frame.samples().chunks_exact(channel_count) {
            let mono = samples.iter().copied().sum::<f32>() / channel_count as f32;
            if self.downsample_phase_samples == 0 {
                self.resampled_samples.push(mono);
            }
            self.downsample_phase_samples = (self.downsample_phase_samples + 1) % downsample_ratio;
        }

        let resampled_samples = std::mem::take(&mut self.resampled_samples);
        let result = self.push_source_frame_samples(
            &resampled_samples,
            frame_lineage,
            sink,
            dropped_chunks_total,
        );
        self.resampled_samples = resampled_samples;
        result
    }

    fn push_source_frame_samples(
        &mut self,
        samples: &[f32],
        frame_lineage: AudioChunkLineage,
        sink: &crossbeam_channel::Sender<AudioChunk>,
        dropped_chunks_total: &AtomicU64,
    ) -> Result<(), CaptureError> {
        if let Some(previous) = self.latest_source_frame {
            if !same_source_interval(previous, frame_lineage) {
                self.reset_for_discontinuity();
            } else if !contiguous_source_interval(previous, frame_lineage) {
                let missing_frames = missing_source_frames(previous, frame_lineage)?;
                for offset in 0..missing_frames {
                    let sequence_number = previous
                        .last_sequence_number
                        .saturating_add(offset)
                        .saturating_add(1);
                    let missing_lineage = AudioChunkLineage {
                        first_sequence_number: sequence_number,
                        last_sequence_number: sequence_number,
                        missing_sequence_count: 1,
                        inserted_silence_samples: SOURCE_FRAME_SAMPLES as u64,
                        timestamp_start_ns: previous
                            .timestamp_end_ns()
                            .saturating_add(offset.saturating_mul(SOURCE_FRAME_DURATION_NS)),
                        duration_ns: SOURCE_FRAME_DURATION_NS,
                        observed_at_ns: frame_lineage.observed_at_ns,
                        polled_at_ns: frame_lineage.polled_at_ns,
                        ..frame_lineage
                    };
                    self.push_samples(
                        &[0.0; SOURCE_FRAME_SAMPLES],
                        missing_lineage,
                        sink,
                        dropped_chunks_total,
                    )?;
                }
            }
        }

        self.push_samples(samples, frame_lineage, sink, dropped_chunks_total)?;
        self.latest_source_frame = Some(frame_lineage);
        Ok(())
    }

    fn push_samples(
        &mut self,
        samples: &[f32],
        lineage: AudioChunkLineage,
        sink: &crossbeam_channel::Sender<AudioChunk>,
        dropped_chunks_total: &AtomicU64,
    ) -> Result<(), CaptureError> {
        self.pending_lineage = Some(merge_source_interval(self.pending_lineage, lineage));
        let mut sink_disconnected = false;
        let pending_lineage = &mut self.pending_lineage;
        self.chunks.push(samples, |index, samples| {
            let rms = (samples.iter().map(|sample| sample * sample).sum::<f32>()
                / samples.len() as f32)
                .sqrt();
            let chunk = AudioChunk {
                samples,
                rms,
                timestamp: Instant::now(),
                index,
                source: SourceRole::Voice,
                lineage: pending_lineage.take(),
            };
            if deliver_chunk(sink, chunk, dropped_chunks_total).is_err() {
                sink_disconnected = true;
            }
        });
        if sink_disconnected {
            return Err(capture_error(
                "deliver PocketStation microphone",
                "Minutes stopped receiving microphone audio",
            ));
        }
        Ok(())
    }
}

fn deliver_chunk(
    sink: &crossbeam_channel::Sender<AudioChunk>,
    chunk: AudioChunk,
    dropped_chunks_total: &AtomicU64,
) -> Result<(), ()> {
    match sink.try_send(chunk) {
        Ok(()) => Ok(()),
        Err(crossbeam_channel::TrySendError::Full(_)) => {
            dropped_chunks_total.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => Err(()),
    }
}

fn same_source_interval(left: AudioChunkLineage, right: AudioChunkLineage) -> bool {
    left.session_id == right.session_id
        && left.source_id == right.source_id
        && left.stem_id == right.stem_id
        && left.clock_id == right.clock_id
        && left.source_generation == right.source_generation
        && left.discontinuity_epoch == right.discontinuity_epoch
        && left.permission_epoch == right.permission_epoch
}

fn contiguous_source_interval(left: AudioChunkLineage, right: AudioChunkLineage) -> bool {
    same_source_interval(left, right)
        && right.first_sequence_number == left.last_sequence_number.saturating_add(1)
        && source_timestamps_plausible(left, right)
}

fn source_timestamps_plausible(left: AudioChunkLineage, right: AudioChunkLineage) -> bool {
    if right.timestamp_start_ns <= left.timestamp_start_ns {
        return false;
    }
    let Some(missing_sequences) = right
        .first_sequence_number
        .checked_sub(left.last_sequence_number.saturating_add(1))
    else {
        return false;
    };
    let expected_start_ns = left
        .timestamp_end_ns()
        .saturating_add(missing_sequences.saturating_mul(SOURCE_FRAME_DURATION_NS));
    right.timestamp_start_ns.abs_diff(expected_start_ns) <= MAX_TIMESTAMP_JITTER.as_nanos() as u64
}

fn missing_source_frames(
    left: AudioChunkLineage,
    right: AudioChunkLineage,
) -> Result<u64, CaptureError> {
    if !source_timestamps_plausible(left, right) {
        return Err(capture_error(
            "preserve PocketStation microphone timeline",
            "source timestamps did not advance within the bounded repair window",
        ));
    }
    let missing_sequences = right
        .first_sequence_number
        .checked_sub(left.last_sequence_number.saturating_add(1))
        .ok_or_else(|| {
            capture_error(
                "preserve PocketStation microphone timeline",
                "source sequence moved backwards or overlapped",
            )
        })?;
    let missing_duration_ns = missing_sequences.saturating_mul(SOURCE_FRAME_DURATION_NS);
    if missing_duration_ns > MAX_SEQUENCE_GAP_DURATION.as_nanos() as u64 {
        return Err(capture_error(
            "preserve PocketStation microphone timeline",
            format!(
                "source gap of {missing_duration_ns} ns exceeds the bounded {} ns repair window",
                MAX_SEQUENCE_GAP_DURATION.as_nanos()
            ),
        ));
    }
    Ok(missing_sequences)
}

fn merge_source_interval(
    current: Option<AudioChunkLineage>,
    next: AudioChunkLineage,
) -> AudioChunkLineage {
    let Some(mut current) = current else {
        return next;
    };
    debug_assert!(same_source_interval(current, next));
    current.last_sequence_number = next.last_sequence_number;
    current.missing_sequence_count = current
        .missing_sequence_count
        .saturating_add(next.missing_sequence_count);
    current.inserted_silence_samples = current
        .inserted_silence_samples
        .saturating_add(next.inserted_silence_samples);
    current.duration_ns = next
        .timestamp_end_ns()
        .saturating_sub(current.timestamp_start_ns);
    current.observed_at_ns = current.observed_at_ns.min(next.observed_at_ns);
    current.polled_at_ns = current.polled_at_ns.max(next.polled_at_ns);
    current
}

fn capture_error(operation: &str, error: impl std::fmt::Display) -> CaptureError {
    CaptureError::Io(std::io::Error::other(format!("{operation}: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn selection(device_id: &str, display_name: &str) -> MicrophoneSelection {
        MicrophoneSelection {
            selector: DeviceSelector::id(DeviceId::new(device_id)),
            device_id: device_id.to_owned(),
            display_name: display_name.to_owned(),
        }
    }

    fn activity(
        observed_at_ns: u64,
        first_frame_received_at_ns: Option<u64>,
        latest_frame_received_at_ns: Option<u64>,
    ) -> SessionSourceActivityObservations {
        SessionSourceActivityObservations {
            session_started_at_ns: 1,
            observed_at_ns,
            first_frame_received_at_ns,
            latest_frame_received_at_ns,
            frames_received_total: u64::from(first_frame_received_at_ns.is_some()),
        }
    }

    fn policies() -> (SessionSourceActivityPolicy, SessionSourceSignalPolicy) {
        (
            SessionSourceActivityPolicy::new(FIRST_FRAME_TIMEOUT, STALL_TIMEOUT).unwrap(),
            SessionSourceSignalPolicy::new(MINIMUM_PEAK_DBFS, MINIMUM_RMS_DBFS, EXACT_ZERO_TIMEOUT)
                .unwrap(),
        )
    }

    #[test]
    fn activity_facts_distinguish_waiting_no_frames_active_and_stalled() {
        let (activity_policy, signal_policy) = policies();
        let mut low_signal_tracker = LowSignalTracker::default();
        assert_eq!(
            evaluate_observations(
                Some(activity(1_000_000_000, None, None)),
                None,
                activity_policy,
                signal_policy,
                &mut low_signal_tracker,
                1,
                0,
                false,
            ),
            MicrophoneSignalState::AwaitingFirstFrame
        );
        assert_eq!(
            evaluate_observations(
                Some(activity(3_000_000_000, None, None)),
                None,
                activity_policy,
                signal_policy,
                &mut low_signal_tracker,
                1,
                0,
                false,
            ),
            MicrophoneSignalState::NoFrames
        );
        assert_eq!(
            evaluate_observations(
                Some(activity(
                    1_500_000_000,
                    Some(500_000_000),
                    Some(1_000_000_000),
                )),
                None,
                activity_policy,
                signal_policy,
                &mut low_signal_tracker,
                1,
                0,
                false,
            ),
            MicrophoneSignalState::Active
        );
        assert_eq!(
            evaluate_observations(
                Some(activity(
                    4_000_000_000,
                    Some(500_000_000),
                    Some(1_000_000_000),
                )),
                None,
                activity_policy,
                signal_policy,
                &mut low_signal_tracker,
                1,
                0,
                false,
            ),
            MicrophoneSignalState::Stalled
        );
    }

    #[test]
    fn source_failure_is_not_relabelled_as_silence() {
        let (activity_policy, signal_policy) = policies();
        let mut low_signal_tracker = LowSignalTracker::default();
        assert_eq!(
            evaluate_observations(
                None,
                None,
                activity_policy,
                signal_policy,
                &mut low_signal_tracker,
                1,
                0,
                true,
            ),
            MicrophoneSignalState::SourceFailed
        );
    }

    #[test]
    fn duplicate_default_display_name_resolves_by_exact_device_identity() {
        let selected = select_discovered_microphone(
            vec![
                selection("other-uid", "MacBook Pro Microphone"),
                selection("default-uid", "MacBook Pro Microphone"),
            ],
            "MacBook Pro Microphone",
            Some("default-uid"),
        )
        .unwrap();

        assert_eq!(selected.device_id, "default-uid");
    }

    #[test]
    fn explicit_ambiguous_display_name_remains_rejected() {
        let result = select_discovered_microphone(
            vec![
                selection("first-uid", "Duplicate Microphone"),
                selection("second-uid", "Duplicate Microphone"),
            ],
            "Duplicate Microphone",
            None,
        );

        assert!(result.is_err());
    }

    #[test]
    fn fallback_follows_an_os_default_that_changed_physical_device() {
        let fallback = choose_recovery_fallback(
            Some("headset-uid"),
            Some(selection("builtin-uid", "MacBook Microphone")),
        )
        .unwrap();

        assert_eq!(fallback.device_id, "builtin-uid");
        assert_eq!(fallback.display_name, "MacBook Microphone");
    }

    #[test]
    fn fallback_does_not_invent_an_alternative_when_os_default_is_unchanged() {
        assert!(choose_recovery_fallback(
            Some("headset-uid"),
            Some(selection("headset-uid", "Logi HFP")),
        )
        .is_none());
        assert!(choose_recovery_fallback(
            None,
            Some(selection("builtin-uid", "Built-in Microphone")),
        )
        .is_none());
    }

    #[test]
    fn active_zero_samples_are_distinct_from_no_frames_and_useful_signal() {
        assert_eq!(
            classify_evaluations(
                Some(SessionSourceActivityState::Active),
                Some(SessionSourceSignalState::SustainedExactDigitalZero),
                false,
            ),
            MicrophoneSignalState::DigitallySilent
        );
        assert_eq!(
            classify_evaluations(
                Some(SessionSourceActivityState::Active),
                Some(SessionSourceSignalState::MeetsCallerThresholds),
                false,
            ),
            MicrophoneSignalState::SignalObserved
        );
    }

    #[test]
    fn reported_hfp_noise_envelope_requires_bounded_low_signal_warning() {
        let mut tracker = LowSignalTracker::default();
        let timeout_ns = UNUSABLE_LOW_SIGNAL_TIMEOUT.as_nanos() as u64;

        assert!(!tracker.observe(1, 1, 0, Some(-78.3), Some(-91.0)));
        assert!(!tracker.observe(timeout_ns, 1, 0, Some(-78.3), Some(-91.0)));
        assert!(tracker.observe(timeout_ns + 1, 1, 0, Some(-78.3), Some(-91.0)));

        // Ordinary quiet audio and a new physical source both reset the
        // bounded interval rather than inheriting an earlier failure.
        assert!(!tracker.observe(timeout_ns + 2, 1, 0, Some(-60.0), Some(-68.0)));
        assert!(!tracker.observe(timeout_ns * 3, 2, 1, Some(-78.3), Some(-91.0)));
    }

    #[test]
    fn reported_hfp_envelope_transitions_only_after_timeout_and_resets_on_source_change() {
        let mut tracker = LowSignalTracker::default();
        let timeout_ns = UNUSABLE_LOW_SIGNAL_TIMEOUT.as_nanos() as u64;
        let low_signal = Some(SessionSourceSignalEvaluation {
            state: SessionSourceSignalState::BelowCallerThresholds,
            peak_dbfs: Some(-78.3),
            rms_dbfs: Some(-91.0),
            consecutive_exact_zero_duration_ns: 0,
        });
        let evaluate =
            |observed_at_ns, generation, discontinuity, tracker: &mut LowSignalTracker| {
                evaluate_state(
                    Some(SessionSourceActivityState::Active),
                    low_signal,
                    observed_at_ns,
                    tracker,
                    generation,
                    discontinuity,
                    false,
                )
            };

        assert_eq!(
            evaluate(1, 1, 0, &mut tracker),
            MicrophoneSignalState::BelowThresholds
        );
        assert_eq!(
            evaluate(timeout_ns, 1, 0, &mut tracker),
            MicrophoneSignalState::BelowThresholds
        );
        assert_eq!(
            evaluate(timeout_ns + 1, 1, 0, &mut tracker),
            MicrophoneSignalState::SustainedLowSignal
        );

        assert_eq!(
            evaluate(timeout_ns * 3, 2, 0, &mut tracker),
            MicrophoneSignalState::BelowThresholds,
            "a new source generation starts a new bounded interval"
        );
        assert_eq!(
            evaluate(timeout_ns * 5, 2, 1, &mut tracker),
            MicrophoneSignalState::BelowThresholds,
            "a discontinuity starts a new bounded interval"
        );
    }

    #[test]
    fn sustained_low_signal_warns_but_does_not_authorize_source_recovery() {
        let observations = MicrophoneObservations {
            state: MicrophoneSignalState::SustainedLowSignal,
            ..MicrophoneObservations::default()
        };

        assert!(!observations.requires_recovery());
        assert!(!observations.confirms_recovery());
    }

    #[test]
    fn stale_good_window_cannot_confirm_a_replacement_without_a_current_frame() {
        let (activity_policy, _) = policies();
        let mut continuity = ObservationContinuityTracker::default();
        let old_activity = SessionSourceActivityObservations {
            session_started_at_ns: 1,
            observed_at_ns: 1_000_000_000,
            first_frame_received_at_ns: Some(900_000_000),
            latest_frame_received_at_ns: Some(990_000_000),
            frames_received_total: 1,
        };
        let old_good_window = SignalWindowContinuity {
            source_generation: 1,
            discontinuity_epoch: 0,
            samples_observed_total: 480,
        };

        let (initial_activity, initial_signal_is_current) =
            continuity.project_activity(Some(old_activity), Some(old_good_window), 1, 0);
        assert!(initial_signal_is_current);
        assert_eq!(
            initial_activity.unwrap().evaluate(activity_policy).state,
            SessionSourceActivityState::Active
        );
        assert!(MicrophoneObservations {
            state: MicrophoneSignalState::SignalObserved,
            source_generation: 1,
            discontinuity_epoch: 0,
            ..MicrophoneObservations::default()
        }
        .confirms_recovery());

        let (replacement_activity, replacement_signal_is_current) = continuity.project_activity(
            Some(SessionSourceActivityObservations {
                observed_at_ns: 1_100_000_000,
                ..old_activity
            }),
            Some(old_good_window),
            2,
            1,
        );
        assert!(!replacement_signal_is_current);
        let replacement_state = classify_evaluations(
            replacement_activity.map(|entry| entry.evaluate(activity_policy).state),
            None,
            false,
        );
        assert_eq!(replacement_state, MicrophoneSignalState::AwaitingFirstFrame);
        assert!(!MicrophoneObservations {
            state: replacement_state,
            source_generation: 2,
            discontinuity_epoch: 1,
            ..MicrophoneObservations::default()
        }
        .confirms_recovery());

        let (current_activity, current_signal_is_current) = continuity.project_activity(
            Some(SessionSourceActivityObservations {
                observed_at_ns: 1_200_000_000,
                latest_frame_received_at_ns: Some(1_190_000_000),
                frames_received_total: 2,
                ..old_activity
            }),
            Some(SignalWindowContinuity {
                source_generation: 2,
                discontinuity_epoch: 1,
                samples_observed_total: 960,
            }),
            2,
            1,
        );
        assert!(current_signal_is_current);
        assert_eq!(
            current_activity.unwrap().evaluate(activity_policy).state,
            SessionSourceActivityState::Active
        );
        assert!(MicrophoneObservations {
            state: MicrophoneSignalState::SignalObserved,
            source_generation: 2,
            discontinuity_epoch: 1,
            ..MicrophoneObservations::default()
        }
        .confirms_recovery());
    }

    #[test]
    fn opened_format_diagnostic_is_content_free_and_names_the_physical_format() {
        let identity = MicrophoneDiagnosticIdentity {
            device_id: "device-uid".to_owned(),
            display_name: "Logi HFP".to_owned(),
        };
        let native_format = CaptureNativeFormat {
            sample_rate_hz: 16_000,
            channel_count: 1,
            sample_representation: pocketstation::CaptureSampleRepresentation::SignedInteger16,
        };
        let entry = opened_native_format_log_entry(&identity, native_format, 2, 1);

        assert_eq!(entry["step"], "pocketstation_microphone_opened_format");
        assert_eq!(entry["device_id"], "device-uid");
        assert_eq!(entry["sample_rate_hz"], 16_000);
        assert_eq!(entry["channel_count"], 1);
        assert_eq!(entry["sample_representation"], "SignedInteger16");
        assert_eq!(entry["source_generation"], 2);
        assert_eq!(entry["discontinuity_epoch"], 1);
        assert!(entry.get("samples").is_none());
        assert!(entry.get("audio").is_none());

        let observations = MicrophoneObservations {
            native_format: Some(native_format),
            source_generation: 2,
            discontinuity_epoch: 1,
            ..MicrophoneObservations::default()
        };
        let key = opened_native_format_diagnostic_key(observations, &identity);
        assert_eq!(
            opened_native_format_diagnostic_key(observations, &identity),
            key.clone(),
            "unchanged observations are de-duplicated by the same key"
        );
        assert_eq!(
            opened_native_format_diagnostic_key(
                MicrophoneObservations {
                    discontinuity_epoch: 2,
                    ..observations
                },
                &identity,
            ),
            key.clone(),
            "a discontinuity alone does not duplicate the attachment diagnostic"
        );
        assert_ne!(
            opened_native_format_diagnostic_key(
                MicrophoneObservations {
                    source_generation: 3,
                    ..observations
                },
                &identity,
            ),
            key.clone(),
            "a new source generation gets one new diagnostic"
        );
        assert_ne!(
            opened_native_format_diagnostic_key(
                observations,
                &MicrophoneDiagnosticIdentity {
                    device_id: "fallback-uid".to_owned(),
                    display_name: "Built-in Microphone".to_owned(),
                },
            ),
            key,
            "a different physical device gets one new diagnostic"
        );
    }

    #[test]
    fn aggregate_lineage_preserves_bounds_and_rejects_generation_merging() {
        let first = AudioChunkLineage {
            session_id: 1,
            source_id: 202,
            stem_id: 7,
            clock_id: 9,
            first_sequence_number: 20,
            last_sequence_number: 20,
            missing_sequence_count: 0,
            inserted_silence_samples: 0,
            timestamp_start_ns: 1_000_000_000,
            duration_ns: 10_000_000,
            source_generation: 1,
            discontinuity_epoch: 0,
            permission_epoch: 3,
            observed_at_ns: 1_010_000_000,
            polled_at_ns: 1_011_000_000,
        };
        let mut aggregate = first;
        for sequence in 21..=29 {
            let next = AudioChunkLineage {
                first_sequence_number: sequence,
                last_sequence_number: sequence,
                timestamp_start_ns: 1_000_000_000 + (sequence - 20) * 10_000_000,
                observed_at_ns: 1_010_000_000 + (sequence - 20) * 10_000_000,
                polled_at_ns: 1_011_000_000 + (sequence - 20) * 10_000_000,
                ..first
            };
            assert!(contiguous_source_interval(aggregate, next));
            aggregate = merge_source_interval(Some(aggregate), next);
        }

        assert_eq!(aggregate.first_sequence_number, 20);
        assert_eq!(aggregate.last_sequence_number, 29);
        assert_eq!(aggregate.timestamp_start_ns, 1_000_000_000);
        assert_eq!(aggregate.duration_ns, 100_000_000);
        assert_eq!(aggregate.source_generation, 1);
        assert_eq!(aggregate.discontinuity_epoch, 0);

        let replacement = AudioChunkLineage {
            source_id: 204,
            first_sequence_number: 0,
            last_sequence_number: 0,
            timestamp_start_ns: 1_200_000_000,
            source_generation: 2,
            discontinuity_epoch: 1,
            ..first
        };
        assert!(!same_source_interval(aggregate, replacement));

        let sequence_gap = AudioChunkLineage {
            first_sequence_number: 31,
            last_sequence_number: 31,
            timestamp_start_ns: aggregate.timestamp_end_ns() + 10_000_000,
            ..aggregate
        };
        assert!(!contiguous_source_interval(aggregate, sequence_gap));
    }

    #[test]
    fn source_gap_is_bounded_by_sequence_while_preserving_native_timestamps() {
        let first = AudioChunkLineage {
            session_id: 1,
            source_id: 202,
            stem_id: 7,
            clock_id: 9,
            first_sequence_number: 20,
            last_sequence_number: 20,
            missing_sequence_count: 0,
            inserted_silence_samples: 0,
            timestamp_start_ns: 1_000_000_000,
            duration_ns: SOURCE_FRAME_DURATION_NS,
            source_generation: 1,
            discontinuity_epoch: 0,
            permission_epoch: 3,
            observed_at_ns: 1_010_000_000,
            polled_at_ns: 1_011_000_000,
        };
        let one_missing_frame = AudioChunkLineage {
            first_sequence_number: 22,
            last_sequence_number: 22,
            timestamp_start_ns: 1_020_000_000,
            ..first
        };
        assert_eq!(missing_source_frames(first, one_missing_frame).unwrap(), 1);

        let inconsistent = AudioChunkLineage {
            first_sequence_number: 23,
            last_sequence_number: 23,
            ..one_missing_frame
        };
        assert!(missing_source_frames(first, inconsistent).is_err());

        let jittered_gap = AudioChunkLineage {
            first_sequence_number: 23,
            last_sequence_number: 23,
            timestamp_start_ns: 1_030_000_000 - 106_167,
            ..first
        };
        assert_eq!(missing_source_frames(first, jittered_gap).unwrap(), 2);

        let jittered = AudioChunkLineage {
            first_sequence_number: 21,
            last_sequence_number: 21,
            timestamp_start_ns: first.timestamp_end_ns().saturating_sub(106_167),
            ..first
        };
        assert!(contiguous_source_interval(first, jittered));

        let repeated_start = AudioChunkLineage {
            first_sequence_number: 21,
            last_sequence_number: 21,
            timestamp_start_ns: first.timestamp_start_ns,
            ..first
        };
        assert!(!contiguous_source_interval(first, repeated_start));
        assert!(missing_source_frames(first, repeated_start).is_err());

        let unbounded_timestamp = AudioChunkLineage {
            first_sequence_number: 21,
            last_sequence_number: 21,
            timestamp_start_ns: first
                .timestamp_end_ns()
                .saturating_add(MAX_SEQUENCE_GAP_DURATION.as_nanos() as u64)
                .saturating_add(1),
            ..first
        };
        assert!(!contiguous_source_interval(first, unbounded_timestamp));
        assert!(missing_source_frames(first, unbounded_timestamp).is_err());

        let adjacent_but_far_away = AudioChunkLineage {
            first_sequence_number: 21,
            last_sequence_number: 21,
            timestamp_start_ns: first.timestamp_end_ns().saturating_add(1_000_000_000),
            ..first
        };
        assert!(!contiguous_source_interval(first, adjacent_but_far_away));
        assert!(missing_source_frames(first, adjacent_but_far_away).is_err());

        let unbounded = AudioChunkLineage {
            first_sequence_number: 222,
            last_sequence_number: 222,
            ..first
        };
        assert!(missing_source_frames(first, unbounded).is_err());
    }

    #[test]
    fn inserted_gap_silence_is_written_and_recorded_in_chunk_lineage() {
        let (sender, receiver) = crossbeam_channel::bounded(2);
        let dropped = AtomicU64::new(0);
        let mut writer = MicrophoneAudioChunkWriter::default();
        let template = AudioChunkLineage {
            session_id: 1,
            source_id: 202,
            stem_id: 7,
            clock_id: 9,
            first_sequence_number: 0,
            last_sequence_number: 0,
            missing_sequence_count: 0,
            inserted_silence_samples: 0,
            timestamp_start_ns: 1_000_000_000,
            duration_ns: SOURCE_FRAME_DURATION_NS,
            source_generation: 1,
            discontinuity_epoch: 0,
            permission_epoch: 3,
            observed_at_ns: 1_010_000_000,
            polled_at_ns: 1_011_000_000,
        };

        for sequence in 0..10 {
            let lineage = AudioChunkLineage {
                first_sequence_number: sequence,
                last_sequence_number: sequence,
                timestamp_start_ns: template.timestamp_start_ns
                    + sequence * SOURCE_FRAME_DURATION_NS,
                ..template
            };
            writer
                .push_source_frame_samples(&[0.5; SOURCE_FRAME_SAMPLES], lineage, &sender, &dropped)
                .unwrap();
        }
        let first_chunk = receiver.try_recv().unwrap();
        assert_eq!(first_chunk.lineage.unwrap().last_sequence_number, 9);

        // Skip sequence 10 and its 10 ms timestamp interval exactly at an
        // emitted 100 ms chunk boundary. Gap detection must use the separate
        // latest-source-frame cursor rather than pending chunk aggregation.
        for sequence in 11..20 {
            let lineage = AudioChunkLineage {
                first_sequence_number: sequence,
                last_sequence_number: sequence,
                timestamp_start_ns: template.timestamp_start_ns
                    + sequence * SOURCE_FRAME_DURATION_NS,
                ..template
            };
            writer
                .push_source_frame_samples(&[0.5; SOURCE_FRAME_SAMPLES], lineage, &sender, &dropped)
                .unwrap();
        }

        let chunk = receiver.try_recv().unwrap();
        assert_eq!(chunk.samples.len(), crate::streaming::CHUNK_SAMPLES);
        assert!(chunk.samples[..SOURCE_FRAME_SAMPLES]
            .iter()
            .all(|sample| *sample == 0.0));
        assert!(chunk.samples[SOURCE_FRAME_SAMPLES..]
            .iter()
            .all(|sample| *sample == 0.5));
        let lineage = chunk.lineage.unwrap();
        assert_eq!(lineage.first_sequence_number, 10);
        assert_eq!(lineage.last_sequence_number, 19);
        assert_eq!(lineage.missing_sequence_count, 1);
        assert_eq!(
            lineage.inserted_silence_samples,
            SOURCE_FRAME_SAMPLES as u64
        );
        assert_eq!(lineage.duration_ns, 10 * SOURCE_FRAME_DURATION_NS);
    }

    #[test]
    fn drop_order_requests_stop_then_observes_cancel_before_join_returns() {
        let stop = Arc::new(AtomicBool::new(false));
        let cancel_completed = Arc::new(AtomicBool::new(false));
        let cancel_succeeded = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_cancel_completed = Arc::clone(&cancel_completed);
        let worker_cancel_succeeded = Arc::clone(&cancel_succeeded);
        let (finished_sender, finished) = crossbeam_channel::bounded(1);
        let worker = std::thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            worker_cancel_succeeded.store(true, Ordering::Release);
            worker_cancel_completed.store(true, Ordering::Release);
            finished_sender.send(()).unwrap();
        });
        let started = Instant::now();

        assert!(stop_cancel_and_join(
            &stop,
            Some(worker),
            &cancel_completed,
            &cancel_succeeded,
            &finished,
            Duration::from_secs(1),
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(stop.load(Ordering::Acquire));
        assert!(cancel_completed.load(Ordering::Acquire));
        assert!(cancel_succeeded.load(Ordering::Acquire));
    }

    #[test]
    fn completed_but_unsuccessful_cancellation_is_not_reported_clean() {
        let stop = Arc::new(AtomicBool::new(false));
        let cancel_completed = Arc::new(AtomicBool::new(false));
        let cancel_succeeded = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_cancel_completed = Arc::clone(&cancel_completed);
        let (finished_sender, finished) = crossbeam_channel::bounded(1);
        let worker = std::thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            worker_cancel_completed.store(true, Ordering::Release);
            finished_sender.send(()).unwrap();
        });

        assert!(!stop_cancel_and_join(
            &stop,
            Some(worker),
            &cancel_completed,
            &cancel_succeeded,
            &finished,
            Duration::from_secs(1),
        ));
        assert!(cancel_completed.load(Ordering::Acquire));
        assert!(!cancel_succeeded.load(Ordering::Acquire));
    }

    #[test]
    fn shutdown_wait_is_bounded_when_worker_does_not_finish() {
        let stop = Arc::new(AtomicBool::new(false));
        let cancel_completed = Arc::new(AtomicBool::new(false));
        let cancel_succeeded = Arc::new(AtomicBool::new(false));
        let (_finished_sender, finished) = crossbeam_channel::bounded(1);
        let worker = std::thread::spawn(|| std::thread::sleep(Duration::from_millis(200)));
        let started = Instant::now();

        assert!(!stop_cancel_and_join(
            &stop,
            Some(worker),
            &cancel_completed,
            &cancel_succeeded,
            &finished,
            Duration::from_millis(10),
        ));
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn saturated_minutes_consumer_drops_without_blocking_microphone_worker() {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        let dropped = AtomicU64::new(0);
        let chunk = || AudioChunk {
            samples: vec![0.25; crate::streaming::CHUNK_SAMPLES],
            rms: 0.25,
            timestamp: Instant::now(),
            index: 0,
            source: SourceRole::Voice,
            lineage: None,
        };

        assert_eq!(deliver_chunk(&sender, chunk(), &dropped), Ok(()));
        let started = Instant::now();
        assert_eq!(deliver_chunk(&sender, chunk(), &dropped), Ok(()));
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        assert_eq!(receiver.len(), 1);
    }
}
