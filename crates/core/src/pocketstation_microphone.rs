mod chunks;
mod health;
mod recovery;
mod selection;

use self::chunks::MicrophoneAudioChunkWriter;
#[cfg(test)]
use self::chunks::{
    contiguous_source_interval, deliver_chunk, merge_source_interval, missing_source_frames,
    same_source_interval, MAX_SEQUENCE_GAP_DURATION, SOURCE_FRAME_DURATION_NS,
    SOURCE_FRAME_SAMPLES,
};
#[cfg(test)]
use self::health::{
    classify_evaluations, evaluate_state, LowSignalTracker, ObservationContinuityTracker,
    SignalWindowContinuity, UNUSABLE_LOW_SIGNAL_TIMEOUT,
};
use self::health::{
    MicrophoneObservationEvaluator, EXACT_ZERO_TIMEOUT, FIRST_FRAME_TIMEOUT, MINIMUM_PEAK_DBFS,
    MINIMUM_RMS_DBFS, STALL_TIMEOUT,
};
pub(crate) use self::health::{MicrophoneObservations, MicrophoneSignalState};
#[cfg(test)]
use self::recovery::MicrophoneReplacementOutcome;
pub(crate) use self::recovery::MicrophoneReplacementReconciliation;
use self::recovery::{
    replacement_continuity_reached, MicrophoneControl, PendingMicrophoneReplacement,
};
#[cfg(test)]
use self::selection::{choose_recovery_fallback, select_discovered_microphone};
use self::selection::{MicrophoneDiagnosticIdentity, MicrophoneSelection};
use crate::error::CaptureError;
use crate::streaming::AudioChunk;
#[cfg(test)]
use crate::streaming::{AudioChunkLineage, SourceRole};
use pocketstation::{
    AudioFrameDuration, CaptureNativeFormat, DeviceSelector, Session, SessionEventKind,
    SessionSourceActivityPolicy, SessionSourceReplacementError, SessionSourceSignalPolicy, Source,
    StemId,
};
#[cfg(test)]
use pocketstation::{
    DeviceId, SessionSourceActivityObservations, SessionSourceActivityState,
    SessionSourceReplacementObservations, SessionSourceSignalEvaluation, SessionSourceSignalState,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

const AUDIO_POLL_TIMEOUT: Duration = Duration::from_millis(50);
const WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const OUTPUT_QUEUE_CAPACITY_CHUNKS: usize = 64;
const CONTROL_QUEUE_CAPACITY: usize = 4;

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
    pending_replacement: Option<Box<PendingMicrophoneReplacement>>,
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
            pending_replacement: None,
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
                let mut observation_evaluator =
                    MicrophoneObservationEvaluator::new(activity_policy, signal_policy);
                let mut diagnostic_identity = diagnostic_identity;
                let mut pending_diagnostic_identity = None;
                let mut logged_native_format_attachment = None;
                let mut pending_format_continuity = None;

                while !stop.load(Ordering::Relaxed) {
                    while let Ok(command) = control.try_recv() {
                        source_failed.store(false, Ordering::Relaxed);
                        let continuity_before_request = observations
                            .lock()
                            .map(|entry| {
                                (
                                    entry.source_generation,
                                    entry.discontinuity_epoch,
                                )
                            })
                            .unwrap_or((1, 0));
                        let (result, identity, response) = match command {
                            MicrophoneControl::Reopen {
                                selector,
                                identity,
                                response,
                            } => (
                                running
                                    .reopen_microphone_source(stem_id, selector),
                                identity,
                                response,
                            ),
                            MicrophoneControl::Replace {
                                selector,
                                identity,
                                response,
                            } => (
                                running
                                    .replace_microphone_source(stem_id, selector),
                                identity,
                                response,
                            ),
                        };
                        match &result {
                            Ok(replacement) => {
                                diagnostic_identity = identity;
                                pending_diagnostic_identity = None;
                                logged_native_format_attachment = None;
                                pending_format_continuity = Some((
                                    replacement.source_generation,
                                    replacement.discontinuity_epoch,
                                ));
                            }
                            Err(SessionSourceReplacementError::ResponseTimedOut { .. }) => {
                                pending_diagnostic_identity = Some((
                                    identity,
                                    continuity_before_request.0.saturating_add(1),
                                    continuity_before_request.1.saturating_add(1),
                                ));
                            }
                            Err(_) => {}
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

                    if let Some(current_observations) = observation_evaluator.update_observations(
                        &running,
                        stem_id,
                        source_failed.load(Ordering::Relaxed),
                        &observations,
                    ) {
                        if pending_diagnostic_identity.as_ref().is_some_and(
                            |(_, source_generation, discontinuity_epoch)| {
                                replacement_continuity_reached(
                                    current_observations,
                                    *source_generation,
                                    *discontinuity_epoch,
                                )
                            },
                        ) {
                            if let Some((identity, source_generation, discontinuity_epoch)) =
                                pending_diagnostic_identity.take()
                            {
                                diagnostic_identity = identity;
                                logged_native_format_attachment = None;
                                pending_format_continuity =
                                    Some((source_generation, discontinuity_epoch));
                            }
                        }
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

    pub(crate) fn device_id(&self) -> &str {
        &self.device_id
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
        let mut evaluator = MicrophoneObservationEvaluator::new(activity_policy, signal_policy);
        assert_eq!(
            evaluator.evaluate_observations(
                Some(activity(1_000_000_000, None, None)),
                None,
                1,
                0,
                false,
            ),
            MicrophoneSignalState::AwaitingFirstFrame
        );
        assert_eq!(
            evaluator.evaluate_observations(
                Some(activity(3_000_000_000, None, None)),
                None,
                1,
                0,
                false,
            ),
            MicrophoneSignalState::NoFrames
        );
        assert_eq!(
            evaluator.evaluate_observations(
                Some(activity(
                    1_500_000_000,
                    Some(500_000_000),
                    Some(1_000_000_000),
                )),
                None,
                1,
                0,
                false,
            ),
            MicrophoneSignalState::Active
        );
        assert_eq!(
            evaluator.evaluate_observations(
                Some(activity(
                    4_000_000_000,
                    Some(500_000_000),
                    Some(1_000_000_000),
                )),
                None,
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
        let mut evaluator = MicrophoneObservationEvaluator::new(activity_policy, signal_policy);
        assert_eq!(
            evaluator.evaluate_observations(None, None, 1, 0, true),
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

    #[test]
    fn replacement_timeout_remains_pending_until_new_continuity_is_observed() {
        let outcome = MicrophoneReplacementOutcome::Pending {
            source_generation: 2,
            discontinuity_epoch: 1,
            timeout_ms: 1_000,
        };
        assert!(outcome.is_pending());
        assert_eq!(outcome.continuity(), (2, 1));

        let before = MicrophoneObservations {
            source_generation: 1,
            discontinuity_epoch: 0,
            ..MicrophoneObservations::default()
        };
        assert!(!replacement_continuity_reached(before, 2, 1));

        let after = MicrophoneObservations {
            source_generation: 2,
            discontinuity_epoch: 1,
            replacement: Some(SessionSourceReplacementObservations {
                stem_id: StemId::new(1),
                attempts_total: 1,
                completed_total: 0,
                failed_before_attach_total: 0,
                response_timeouts_total: 1,
                attached_source_id: None,
                source_generation: 2,
                discontinuity_epoch: 1,
                latest_completed_at_ns: None,
            }),
            ..MicrophoneObservations::default()
        };
        assert!(!replacement_continuity_reached(after, 2, 1));

        let attached = MicrophoneObservations {
            replacement: Some(SessionSourceReplacementObservations {
                attached_source_id: Some(pocketstation::SourceId::new(7)),
                completed_total: 1,
                latest_completed_at_ns: Some(5),
                ..after.replacement.expect("replacement observations")
            }),
            ..after
        };
        assert!(replacement_continuity_reached(attached, 2, 1));

        let later_attachment = MicrophoneObservations {
            source_generation: 3,
            discontinuity_epoch: 2,
            replacement: Some(SessionSourceReplacementObservations {
                source_generation: 3,
                discontinuity_epoch: 2,
                ..attached.replacement.expect("replacement observations")
            }),
            ..attached
        };
        assert!(
            !replacement_continuity_reached(later_attachment, 2, 1),
            "a later replacement must not be labelled with an older pending device identity"
        );
    }
}
