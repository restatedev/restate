use std::assert_matches;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn token() -> String {
    "test-token".to_owned()
}

fn ok_source() -> Credential {
    MockSource::new(|_| MockOutcome::Token(token()))
}

fn permanently_failing_source() -> Credential {
    MockSource::new(|_| MockOutcome::Error(permanent_error("impersonation misconfigured")))
}

#[test]
fn ambient_unsupported_error_is_actionable_and_leak_free() {
    let err = GcpAuthError::AmbientUnsupported {
        audience: "https://svc-abc-uc.a.run.app".into(),
    };
    let msg = err.to_string();
    // Actionable: names the audience and the fix.
    assert!(msg.contains("https://svc-abc-uc.a.run.app"), "{msg}");
    assert!(msg.contains("--gcp-impersonate-service-account"), "{msg}");
    // Leak-free: must not surface the google-cloud-auth internal API hint.
    assert!(!msg.contains("idtoken::user_account"), "{msg}");
    assert!(!msg.to_lowercase().contains("builder directly"), "{msg}");
}

struct MockSource {
    calls: AtomicUsize,
    behavior: Mutex<Box<dyn FnMut(usize) -> MockOutcome + Send>>,
}

enum MockOutcome {
    Token(String),
    Error(google_cloud_auth::errors::CredentialsError),
    Hang,
}

impl MockSource {
    fn new(behavior: impl FnMut(usize) -> MockOutcome + Send + 'static) -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            behavior: Mutex::new(Box::new(behavior)),
        })
    }
}

#[async_trait]
impl IdTokenSource for MockSource {
    async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let outcome = (self.behavior.lock())(call);
        match outcome {
            MockOutcome::Token(token) => Ok(token),
            MockOutcome::Error(error) => Err(error),
            MockOutcome::Hang => std::future::pending().await,
        }
    }
}

fn ready_entry(source: Credential) -> Arc<CredentialEntry> {
    let entry = Arc::new(CredentialEntry::new());
    assert!(
        entry
            .build
            .set(futures::future::ready(Ok(source)).boxed().shared())
            .is_ok()
    );
    entry
}

impl IdTokenSpec {
    fn ambient(audience: &str) -> Self {
        IdTokenSpec {
            identity: IdTokenIdentity::Ambient,
            audience: audience.into(),
        }
    }

    fn impersonated(audience: &str, service_account: &str) -> Self {
        IdTokenSpec {
            identity: IdTokenIdentity::impersonated(service_account),
            audience: audience.into(),
        }
    }

    fn federated(audience: &str, provider: &str, service_account: &str) -> Self {
        IdTokenSpec {
            identity: IdTokenIdentity::federated(provider, service_account),
            audience: audience.into(),
        }
    }
}

async fn mint_for_test(identity: IdTokenIdentity, audience: &str) -> Result<String, GcpAuthError> {
    let spec = IdTokenSpec {
        identity,
        audience: audience.into(),
    };
    mint(&spec).await
}

fn transient_error(message: &str) -> google_cloud_auth::errors::CredentialsError {
    google_cloud_auth::errors::CredentialsError::from_msg(true, message)
}

fn permanent_error(message: &str) -> google_cloud_auth::errors::CredentialsError {
    google_cloud_auth::errors::CredentialsError::from_msg(false, message)
}

fn add_build_override(
    cache_key: IdTokenSpec,
    f: impl Fn(&IdTokenSpec) -> Result<Credential, GcpAuthError> + Send + Sync + 'static,
) {
    let f = Arc::new(f);
    credential_registry()
        .test_hooks
        .build_overrides
        .lock()
        .insert(
            cache_key,
            Arc::new(move |spec| {
                let result = f(&spec);
                futures::future::ready(result).boxed()
            }),
        );
}

fn add_async_build_override<F, Fut>(cache_key: IdTokenSpec, f: F)
where
    F: Fn(IdTokenSpec) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Credential, GcpAuthError>> + Send + 'static,
{
    credential_registry()
        .test_hooks
        .build_overrides
        .lock()
        .insert(cache_key, Arc::new(move |spec| f(spec).boxed()));
}

fn add_ambient_source_override(
    f: impl Fn() -> Result<google_cloud_auth::credentials::Credentials, String> + Send + Sync + 'static,
) {
    *credential_registry()
        .test_hooks
        .ambient_source_override
        .lock() = Some(Arc::new(f));
}

#[derive(Clone, Copy, Debug)]
enum ProbeOutcome {
    Healthy,
    Dead,
    Transient,
    Hang,
}

struct FakeCredentialsProvider(Mutex<Box<dyn FnMut() -> ProbeOutcome + Send>>);

impl std::fmt::Debug for FakeCredentialsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FakeCredentialsProvider")
    }
}

impl FakeCredentialsProvider {
    fn always(outcome: impl Fn() -> ProbeOutcome + Send + 'static) -> Self {
        Self(Mutex::new(Box::new(outcome)))
    }
}

impl google_cloud_auth::credentials::CredentialsProvider for FakeCredentialsProvider {
    async fn headers(
        &self,
        _extensions: http::Extensions,
    ) -> std::result::Result<
        google_cloud_auth::credentials::CacheableResource<http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        let outcome = (self.0.lock())();
        match outcome {
            ProbeOutcome::Healthy => Ok(google_cloud_auth::credentials::CacheableResource::New {
                entity_tag: google_cloud_auth::credentials::EntityTag::new(),
                data: http::HeaderMap::new(),
            }),
            ProbeOutcome::Dead => Err(permanent_error("source refresh task permanently dead")),
            ProbeOutcome::Transient => Err(transient_error(
                "source refresh task transiently unavailable",
            )),
            ProbeOutcome::Hang => std::future::pending().await,
        }
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}

fn credential_source(outcome: ProbeOutcome) -> google_cloud_auth::credentials::Credentials {
    google_cloud_auth::credentials::Credentials::from(FakeCredentialsProvider::always(move || {
        outcome
    }))
}

#[restate_core::test]
async fn single_flight_builds_once_under_concurrent_misses() {
    let audience = "https://single-flight.example.com";
    let builds = Arc::new(AtomicUsize::new(0));

    add_build_override(IdTokenSpec::ambient(audience), {
        let builds = builds.clone();
        move |_| {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok(ok_source())
        }
    });

    let results = futures::future::join_all(
        (0..64).map(|_| mint_for_test(IdTokenIdentity::Ambient, audience)),
    )
    .await;

    assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

#[restate_core::test]
async fn failed_shared_build_is_evicted_and_retried() {
    let audience = "https://build-retry.example.com";
    let builds = Arc::new(AtomicUsize::new(0));

    add_build_override(IdTokenSpec::ambient(audience), {
        let builds = builds.clone();
        move |_| match builds.fetch_add(1, Ordering::SeqCst) {
            0 => Err(GcpAuthError::Build {
                audience: audience.to_owned(),
                message: "first build failed".to_owned(),
            }),
            _ => Ok(ok_source()),
        }
    });

    let first = mint_for_test(IdTokenIdentity::Ambient, audience).await;
    assert!(matches!(first, Err(GcpAuthError::Build { .. })));
    let second = mint_for_test(IdTokenIdentity::Ambient, audience).await;
    assert!(second.is_ok(), "{second:?}");
    assert_eq!(builds.load(Ordering::SeqCst), 2);
}

#[restate_core::test(start_paused = true)]
async fn timed_out_caller_leaves_shared_construction_running() {
    let audience = "https://slow-construction.example.com";
    let cache_key = IdTokenSpec::ambient(audience);
    let builds = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Notify::new());

    add_async_build_override(cache_key.clone(), {
        let builds = builds.clone();
        let release = release.clone();
        move |_| {
            let builds = builds.clone();
            let release = release.clone();
            async move {
                builds.fetch_add(1, Ordering::SeqCst);
                release.notified().await;
                Ok(ok_source())
            }
        }
    });

    let first = mint_for_test(IdTokenIdentity::Ambient, audience).await;
    assert!(matches!(first, Err(GcpAuthError::Timeout { .. })));
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    let entry = credential_registry()
        .cache
        .get(&cache_key)
        .await
        .expect("timed-out construction remains cached");

    // notify_one stores a permit if construction was descheduled between incrementing the
    // counter above and registering its waiter.
    release.notify_one();
    let second = mint_for_test(IdTokenIdentity::Ambient, audience).await;
    assert!(second.is_ok(), "{second:?}");
    assert_eq!(builds.load(Ordering::SeqCst), 1);
    let cached = credential_registry().cache.get(&cache_key).await;
    assert!(matches!(cached, Some(current) if Arc::ptr_eq(&current, &entry)));
}

#[restate_core::test(start_paused = true)]
async fn completed_failure_after_caller_timeout_is_rebuilt_for_the_next_caller() {
    let audience = "https://stale-build-failure.example.com";
    let builds = Arc::new(AtomicUsize::new(0));
    let release_first_build = Arc::new(tokio::sync::Notify::new());
    let first_build_finished = Arc::new(tokio::sync::Notify::new());

    add_async_build_override(IdTokenSpec::ambient(audience), {
        let builds = builds.clone();
        let release_first_build = release_first_build.clone();
        let first_build_finished = first_build_finished.clone();
        move |_| {
            let release_first_build = release_first_build.clone();
            let first_build_finished = first_build_finished.clone();
            let build = builds.fetch_add(1, Ordering::SeqCst);
            async move {
                if build == 0 {
                    release_first_build.notified().await;
                    first_build_finished.notify_one();
                    Err(GcpAuthError::Build {
                        audience: audience.to_owned(),
                        message: "first build failed after its caller timed out".to_owned(),
                    })
                } else {
                    Ok(ok_source())
                }
            }
        }
    });

    let first = mint_for_test(IdTokenIdentity::Ambient, audience).await;
    assert!(matches!(first, Err(GcpAuthError::Timeout { .. })));

    release_first_build.notify_one();
    first_build_finished.notified().await;
    let failed_entry = credential_registry()
        .cache
        .get(&IdTokenSpec::ambient(audience))
        .await
        .expect("timed-out construction remains cached");
    tokio::time::timeout(Duration::from_secs(1), async {
        while !failed_entry.completed_build_failed() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("the TaskCenter-owned build publishes its completed failure");

    let second = mint_for_test(IdTokenIdentity::Ambient, audience).await;
    assert!(second.is_ok(), "{second:?}");
    assert_eq!(builds.load(Ordering::SeqCst), 2);
}

#[restate_core::test(start_paused = true)]
async fn construction_and_token_mint_share_one_deadline() {
    let audience = "https://shared-deadline.example.com";

    add_async_build_override(IdTokenSpec::ambient(audience), move |_| async move {
        tokio::time::sleep(Duration::from_secs(4)).await;
        Ok(MockSource::new(|_| MockOutcome::Hang) as Credential)
    });

    let started = Instant::now();
    let outcome = mint_for_test(IdTokenIdentity::Ambient, audience).await;
    assert!(matches!(outcome, Err(GcpAuthError::Timeout { .. })));
    assert_eq!(started.elapsed(), MINT_ATTEMPT_TIMEOUT);
}

#[restate_core::test]
async fn concurrent_ambient_source_resolutions_share_one_build() {
    let build_count = Arc::new(AtomicUsize::new(0));
    add_ambient_source_override({
        let build_count = build_count.clone();
        move || {
            build_count.fetch_add(1, Ordering::SeqCst);
            Ok(credential_source(ProbeOutcome::Healthy))
        }
    });

    let registry = credential_registry();
    let results = futures::future::join_all((0..8).map(|_| registry.ambient_source())).await;

    assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
    assert_eq!(build_count.load(Ordering::SeqCst), 1);
}

#[restate_core::test(start_paused = true)]
async fn credential_source_is_dead_only_for_a_proven_permanent_error() {
    let cases = [
        (ProbeOutcome::Healthy, false),
        (ProbeOutcome::Transient, false),
        (ProbeOutcome::Dead, true),
        (ProbeOutcome::Hang, false),
    ];
    for (outcome, expected_dead) in cases {
        let source = credential_source(outcome);
        assert_eq!(
            credential_source_is_dead(&source).await,
            expected_dead,
            "{outcome:?}"
        );
    }
}

#[restate_core::test(start_paused = true)]
async fn source_recovery_does_not_queue_behind_an_in_progress_operation() {
    let source = RecoverableCredentialSource::new();
    let _operation = source.cell.lock().await;

    let replaced = tokio::time::timeout(
        Duration::from_secs(1),
        source.replace_if_dead(async { unreachable!("a busy source is not rebuilt") }),
    )
    .await
    .expect("redundant recovery returns without waiting")
    .expect("skipping recovery is not an error");
    assert!(!replaced);
}

#[restate_core::test]
async fn dead_shared_source_is_replaced_after_permanent_outer_failure() {
    let service_account = "sa@example.iam.gserviceaccount.com";
    for (name, source_kind) in [
        ("ambient", RecoverySourceUnderTest::Ambient),
        (
            "federated",
            RecoverySourceUnderTest::Federated(
                "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/recovery",
            ),
        ),
    ] {
        let audience = format!("https://{name}-recovery.example.com");
        let fixture = recovery_fixture(
            source_kind,
            &audience,
            service_account,
            credential_source(ProbeOutcome::Dead),
        )
        .await;

        let outcome = mint_for_test(fixture.identity.clone(), &audience).await;
        assert!(
            matches!(&outcome, Err(GcpAuthError::Mint { .. })),
            "{name}: {outcome:?}"
        );
        wait_for_count(&fixture.rebuilds, 1).await;
        assert_eq!(
            fixture.rebuilds.load(Ordering::SeqCst),
            1,
            "the {name} source must be replaced exactly once"
        );

        let reused = fixture
            .source
            .credentials()
            .get_or_build(async { unreachable!("the recovered source must be reused") })
            .await;
        assert!(reused.is_ok(), "{name}: {reused:?}");
        assert_eq!(fixture.rebuilds.load(Ordering::SeqCst), 1);
    }
}

#[restate_core::test(start_paused = true)]
async fn permanent_outer_failure_does_not_await_source_recovery() {
    let service_account = "sa@example.iam.gserviceaccount.com";
    for (name, source_kind) in [
        ("ambient", RecoverySourceUnderTest::Ambient),
        (
            "federated",
            RecoverySourceUnderTest::Federated(
                "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/detached-recovery",
            ),
        ),
    ] {
        let audience = format!("https://detached-{name}-recovery.example.com");
        let fixture = recovery_fixture(
            source_kind,
            &audience,
            service_account,
            credential_source(ProbeOutcome::Hang),
        )
        .await;

        let started = Instant::now();
        let outcome = mint_for_test(fixture.identity, &audience).await;
        assert!(
            matches!(&outcome, Err(GcpAuthError::Mint { .. })),
            "{name}: {outcome:?}"
        );
        assert!(
            started.elapsed() < SOURCE_PROBE_TIMEOUT,
            "mint must not await the {name} source-recovery probe"
        );
    }
}

#[restate_core::test]
async fn healthy_shared_source_is_not_replaced_by_outer_failures() {
    let service_account = "sa@example.iam.gserviceaccount.com";
    for (name, source_kind) in [
        ("ambient", RecoverySourceUnderTest::Ambient),
        (
            "federated",
            RecoverySourceUnderTest::Federated(
                "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/stable",
            ),
        ),
    ] {
        let audience = format!("https://{name}-stable.example.com");
        let probes = Arc::new(AtomicUsize::new(0));
        let fixture = recovery_fixture(
            source_kind,
            &audience,
            service_account,
            healthy_source_with_probe_count(probes.clone()),
        )
        .await;

        for _ in 0..5 {
            let outcome = mint_for_test(fixture.identity.clone(), &audience).await;
            assert!(
                matches!(&outcome, Err(GcpAuthError::Mint { .. })),
                "{name}: {outcome:?}"
            );
        }
        wait_for_count(&probes, 1).await;
        assert_eq!(
            fixture.rebuilds.load(Ordering::SeqCst),
            0,
            "a healthy {name} source must not be replaced by an outer failure"
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum MintFailure {
    Transient,
    Timeout,
    Permanent,
}

#[restate_core::test(start_paused = true)]
async fn mint_failure_policy_controls_cache_eviction() {
    for (name, failure, retained) in [
        ("transient", MintFailure::Transient, true),
        ("timeout", MintFailure::Timeout, true),
        ("permanent", MintFailure::Permanent, false),
    ] {
        let audience = format!("https://{name}.example.com");
        let cache_key = IdTokenSpec::ambient(&audience);
        let source: Credential = match failure {
            MintFailure::Transient => MockSource::new(|call| {
                if call == 0 {
                    MockOutcome::Error(transient_error("temporarily unavailable"))
                } else {
                    MockOutcome::Token(token())
                }
            }),
            MintFailure::Timeout => MockSource::new(|_| MockOutcome::Hang),
            MintFailure::Permanent => {
                MockSource::new(|_| MockOutcome::Error(permanent_error("misconfigured")))
            }
        };
        let entry = ready_entry(source);
        credential_registry()
            .cache
            .insert(cache_key.clone(), entry.clone())
            .await;

        let outcome = mint_for_test(IdTokenIdentity::Ambient, &audience).await;
        match failure {
            MintFailure::Timeout => {
                assert!(matches!(outcome, Err(GcpAuthError::Timeout { .. })))
            }
            MintFailure::Transient | MintFailure::Permanent => {
                assert!(matches!(outcome, Err(GcpAuthError::Mint { .. })))
            }
        }

        let cached = credential_registry().cache.get(&cache_key).await;
        assert_eq!(
            cached.is_some(),
            retained,
            "unexpected cache policy for {name}"
        );
        if retained {
            assert!(matches!(cached, Some(current) if Arc::ptr_eq(&current, &entry)));
        }
        if matches!(failure, MintFailure::Transient) {
            let healed = mint_for_test(IdTokenIdentity::Ambient, &audience).await;
            assert!(healed.is_ok(), "{healed:?}");
        }
    }
}

#[restate_core::test]
async fn aba_race_stale_caller_cannot_evict_or_recover_source() {
    let audience = "https://aba.example.com";
    let service_account = "sa@example.iam.gserviceaccount.com";
    let cache_key = IdTokenSpec::impersonated(audience, service_account);

    credential_registry()
        .ambient_source
        .seed_for_test(credential_source(ProbeOutcome::Dead))
        .await;
    let source_rebuilds = Arc::new(AtomicUsize::new(0));
    add_ambient_source_override({
        let source_rebuilds = source_rebuilds.clone();
        move || {
            source_rebuilds.fetch_add(1, Ordering::SeqCst);
            Ok(credential_source(ProbeOutcome::Healthy))
        }
    });

    let new_source = ok_source();
    let new_entry = ready_entry(new_source);

    struct SwapThenFail {
        spec: IdTokenSpec,
        replacement: Arc<CredentialEntry>,
    }

    #[async_trait]
    impl IdTokenSource for SwapThenFail {
        async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
            credential_registry()
                .cache
                .insert(self.spec.clone(), self.replacement.clone())
                .await;
            Err(permanent_error("old, now gone"))
        }
    }

    let old_source: Arc<dyn IdTokenSource> = Arc::new(SwapThenFail {
        spec: cache_key.clone(),
        replacement: new_entry.clone(),
    });
    let old_entry = ready_entry(old_source);
    credential_registry()
        .cache
        .insert(cache_key.clone(), old_entry)
        .await;

    let outcome = mint_for_test(IdTokenIdentity::impersonated(service_account), audience).await;
    assert!(
        matches!(outcome, Err(GcpAuthError::Mint { .. })),
        "{outcome:?}"
    );

    let cached = credential_registry().cache.get(&cache_key).await;
    assert!(
        matches!(cached, Some(s) if Arc::ptr_eq(&s, &new_entry)),
        "evict from a stale caller must not remove the freshly rebuilt healthy entry"
    );
    assert_eq!(
        source_rebuilds.load(Ordering::SeqCst),
        0,
        "a stale caller must not recover the shared source"
    );
}

#[test]
fn credential_construction_runs_on_task_centers_default_runtime_not_the_callers() {
    use restate_core::TaskCenterFutureExt as _;

    let audience = "https://runtime-affinity.example.com";
    assert_child_survives_caller_runtime(
        "credential construction",
        |task_center, refresh_task| {
            task_center.run_sync(|| {
                add_build_override(IdTokenSpec::ambient(audience), move |_| {
                    // Simulate the library refresh task spawned during construction.
                    refresh_task.spawn();
                    Ok(ok_source())
                });
                credential_registry()
            })
        },
        |caller_runtime, task_center, registry| {
            let registry = *registry;
            let spec = IdTokenSpec::ambient(audience);
            let result = caller_runtime.block_on(
                async {
                    let entry = registry.get_entry(&spec).await;
                    entry.get_or_start(registry, &spec).await
                }
                .in_tc(task_center),
            );
            if let Err(error) = &result {
                panic!("{error}");
            }
        },
    );
}

#[test]
fn credentials_error_403_permission_denied_survives_display_error_chain() {
    let body = br#"{"error":{"code":403,"message":"The caller does not have permission","status":"PERMISSION_DENIED"}}"#;
    let gax_error = google_cloud_gax::error::Error::http(
        403,
        http::HeaderMap::new(),
        bytes::Bytes::from_static(body),
    );
    let credentials_error = google_cloud_auth::errors::CredentialsError::new(
        false,
        "failed to fetch ID token via impersonation",
        gax_error,
    );

    let message = display_error_chain(&credentials_error);
    assert!(message.contains("403"), "{message}");
    assert!(message.contains("PERMISSION_DENIED"), "{message}");
}

struct RuntimeSurvivalTask {
    started_tx: std::sync::mpsc::Sender<()>,
    finished_tx: std::sync::mpsc::Sender<()>,
    finish_barrier: Arc<Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
}

impl RuntimeSurvivalTask {
    fn spawn(&self) {
        let started_tx = self.started_tx.clone();
        let finished_tx = self.finished_tx.clone();
        let finish_barrier = self
            .finish_barrier
            .lock()
            .take()
            .expect("the simulated refresh task spawns once");
        tokio::spawn(async move {
            started_tx.send(()).expect("the test awaits task startup");
            let _ = finish_barrier.await;
            finished_tx
                .send(())
                .expect("the test awaits task completion");
        });
    }
}

fn assert_child_survives_caller_runtime<State>(
    context: &str,
    install_hook: impl FnOnce(&restate_core::Handle, RuntimeSurvivalTask) -> State,
    run_action: impl FnOnce(&tokio::runtime::Runtime, &restate_core::Handle, &State),
) {
    let default_runtime = tokio::runtime::Runtime::new().expect("default runtime builds");
    let task_center = restate_core::TaskCenterBuilder::default()
        .default_runtime_handle(default_runtime.handle().clone())
        .build()
        .expect("task center builds")
        .into_handle();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let (allow_finish_tx, finish_barrier) = tokio::sync::oneshot::channel();
    let refresh_task = RuntimeSurvivalTask {
        started_tx,
        finished_tx,
        finish_barrier: Arc::new(Mutex::new(Some(finish_barrier))),
    };
    let state = install_hook(&task_center, refresh_task);

    {
        let caller_runtime = tokio::runtime::Runtime::new().expect("caller runtime builds");
        run_action(&caller_runtime, &task_center, &state);
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("{context} must spawn the simulated refresh task"));
    }

    allow_finish_tx.send(()).unwrap_or_else(|_| {
        panic!("the task spawned during {context} must survive the caller runtime's drop")
    });
    finished_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_else(|_| {
            panic!("the task spawned during {context} must finish after the caller runtime drops")
        });
}

fn healthy_source_with_probe_count(
    probes: Arc<AtomicUsize>,
) -> google_cloud_auth::credentials::Credentials {
    google_cloud_auth::credentials::Credentials::from(FakeCredentialsProvider::always(move || {
        probes.fetch_add(1, Ordering::SeqCst);
        ProbeOutcome::Healthy
    }))
}

fn counted_access_token_source_override(provider: &str) -> Arc<AtomicUsize> {
    let builds = Arc::new(AtomicUsize::new(0));
    federation::test_hooks::install_access_token_source_override(provider, {
        let builds = builds.clone();
        move || {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok(credential_source(ProbeOutcome::Healthy))
        }
    });
    builds
}

async fn wait_for_count(counter: &AtomicUsize, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background recovery completes promptly");
}

async fn build_federated_source_for_spec(
    sources: &federation::FederatedAccessTokenSourceIndex,
    spec: IdTokenSpec,
) -> Result<Credential, GcpAuthError> {
    let IdTokenSpec { identity, audience } = spec;
    let IdTokenIdentity::Federated {
        provider,
        service_account,
    } = identity
    else {
        panic!("test helper requires a federated spec");
    };
    federation::build_federated_source(
        sources,
        provider.into(),
        service_account.into(),
        audience.into(),
    )
    .await
}

#[derive(Clone, Copy, Debug)]
enum RecoverySourceUnderTest {
    Ambient,
    Federated(&'static str),
}

impl RecoverySourceUnderTest {
    fn identity(self, service_account: &str) -> IdTokenIdentity {
        match self {
            Self::Ambient => IdTokenIdentity::impersonated(service_account),
            Self::Federated(provider) => IdTokenIdentity::federated(provider, service_account),
        }
    }
}

enum RecoverySourceHandle {
    Ambient(&'static RecoverableCredentialSource),
    Federated(Arc<federation::FederatedAccessTokenSource>),
}

impl RecoverySourceHandle {
    fn credentials(&self) -> &RecoverableCredentialSource {
        match self {
            Self::Ambient(source) => source,
            Self::Federated(source) => &source.credentials,
        }
    }
}

struct RecoveryFixture {
    identity: IdTokenIdentity,
    source: RecoverySourceHandle,
    rebuilds: Arc<AtomicUsize>,
}

async fn recovery_fixture(
    source_kind: RecoverySourceUnderTest,
    audience: &str,
    service_account: &str,
    seed: google_cloud_auth::credentials::Credentials,
) -> RecoveryFixture {
    let registry = credential_registry();
    let identity = source_kind.identity(service_account);
    let (source, rebuilds) = match source_kind {
        RecoverySourceUnderTest::Ambient => {
            let rebuilds = Arc::new(AtomicUsize::new(0));
            add_ambient_source_override({
                let rebuilds = rebuilds.clone();
                move || {
                    rebuilds.fetch_add(1, Ordering::SeqCst);
                    Ok(credential_source(ProbeOutcome::Healthy))
                }
            });
            add_build_override(IdTokenSpec::impersonated(audience, service_account), |_| {
                Ok(permanently_failing_source())
            });
            (
                RecoverySourceHandle::Ambient(&registry.ambient_source),
                rebuilds,
            )
        }
        RecoverySourceUnderTest::Federated(provider) => {
            let access_token_source = registry
                .federated_access_token_sources
                .get_or_create(provider);
            let rebuilds = counted_access_token_source_override(provider);
            let outer_credential_lease = access_token_source.clone();
            add_build_override(
                IdTokenSpec::federated(audience, provider, service_account),
                move |_| {
                    Ok(Arc::new(LeasedFailingSource {
                        _access_token_source: outer_credential_lease.clone(),
                    }) as Credential)
                },
            );
            (
                RecoverySourceHandle::Federated(access_token_source),
                rebuilds,
            )
        }
    };
    source.credentials().seed_for_test(seed).await;
    RecoveryFixture {
        identity,
        source,
        rebuilds,
    }
}

struct LeasedFailingSource {
    _access_token_source: Arc<federation::FederatedAccessTokenSource>,
}

#[async_trait]
impl IdTokenSource for LeasedFailingSource {
    async fn id_token(&self) -> Result<String, google_cloud_auth::errors::CredentialsError> {
        Err(permanent_error("impersonation misconfigured"))
    }
}

#[restate_core::test]
async fn outer_credentials_share_release_and_rebuild_one_access_token_source() {
    let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/shared-lease";
    let service_account = "sa@example.iam.gserviceaccount.com";
    let builds = counted_access_token_source_override(provider);

    let registry = credential_registry();
    let spec_a = IdTokenSpec::federated(
        "https://shared-lease-a.example.com",
        provider,
        service_account,
    );
    let spec_b = IdTokenSpec::federated(
        "https://shared-lease-b.example.com",
        provider,
        service_account,
    );
    let outer_a =
        build_federated_source_for_spec(&registry.federated_access_token_sources, spec_a.clone())
            .await
            .expect("outer construction succeeds");
    let outer_b =
        build_federated_source_for_spec(&registry.federated_access_token_sources, spec_b.clone())
            .await
            .expect("outer construction succeeds");
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "both outer credentials must share one access-token source build"
    );

    let weak = registry
        .federated_access_token_sources
        .weak_for_test(provider)
        .expect("the builds above must have created an entry for this provider");

    registry
        .cache
        .insert(spec_a.clone(), ready_entry(outer_a))
        .await;
    registry
        .cache
        .insert(spec_b.clone(), ready_entry(outer_b))
        .await;
    assert_eq!(
        weak.strong_count(),
        2,
        "both cached outer credentials must hold their own lease on the shared source"
    );

    registry.cache.invalidate(&spec_a).await;
    registry.cache.run_pending_tasks().await;
    assert_eq!(
        weak.strong_count(),
        1,
        "evicting one outer credential must drop exactly its own lease"
    );
    assert_eq!(
        registry.federated_access_token_sources.reap_dead(),
        1,
        "the shared source must stay indexed while spec_b's outer credential references it"
    );

    registry.cache.invalidate(&spec_b).await;
    registry.cache.run_pending_tasks().await;
    assert_eq!(
        weak.strong_count(),
        0,
        "evicting the last outer credential must drop the last lease"
    );
    assert_eq!(
        registry.federated_access_token_sources.reap_dead(),
        0,
        "the source must be reaped after its last outer credential releases it"
    );
    assert!(
        registry
            .federated_access_token_sources
            .weak_for_test(provider)
            .is_none(),
        "the map key itself must be gone after reaping, not merely a dead tombstone"
    );

    let spec_c = IdTokenSpec::federated(
        "https://shared-lease-c.example.com",
        provider,
        service_account,
    );
    build_federated_source_for_spec(&registry.federated_access_token_sources, spec_c)
        .await
        .expect("outer construction succeeds");
    assert_eq!(
        builds.load(Ordering::SeqCst),
        2,
        "a provider whose access-token source was fully reaped must build exactly one fresh \
         source on its next reference"
    );
}

#[restate_core::test]
async fn failed_construction_leaves_no_permanently_retained_source() {
    let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/construction-fails";
    let service_account = "sa@example.iam.gserviceaccount.com";
    federation::test_hooks::install_access_token_source_override(provider, || {
        Err("simulated STS exchange failure".to_owned())
    });

    let registry = credential_registry();
    let spec = IdTokenSpec::federated(
        "https://construction-fails.example.com",
        provider,
        service_account,
    );
    let result =
        build_federated_source_for_spec(&registry.federated_access_token_sources, spec).await;
    assert_matches!(result.err(), Some(GcpAuthError::CredentialSource { .. }));

    assert_eq!(
        registry.federated_access_token_sources.reap_dead(),
        0,
        "a failed build's dead weak entry must be pruned by housekeeping, not retained"
    );

    let builds = counted_access_token_source_override(provider);
    let retry_spec = IdTokenSpec::federated(
        "https://construction-fails-retry.example.com",
        provider,
        service_account,
    );
    build_federated_source_for_spec(&registry.federated_access_token_sources, retry_spec)
        .await
        .expect("a fresh lookup must be able to replace the dead tombstone directly");
    assert_eq!(builds.load(Ordering::SeqCst), 1);
}

/// Housekeeping may reap between eviction and recovery. The outer credential that failed still
/// holds its lease at that point, so the reap must leave the source indexed and recoverable.
#[restate_core::test]
async fn reap_during_the_recovery_window_does_not_defeat_recovery() {
    let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/recovery-vs-housekeeping";
    let registry = credential_registry();
    let access_token_source = registry
        .federated_access_token_sources
        .get_or_create(provider);
    access_token_source
        .credentials
        .seed_for_test(credential_source(ProbeOutcome::Dead))
        .await;
    let builds = counted_access_token_source_override(provider);

    // Stands in for the failed outer credential mint() still holds while it recovers.
    let lease = federation::test_hooks::leased_credential(access_token_source);
    registry.federated_access_token_sources.reap_dead();

    registry
        .federated_access_token_sources
        .recover_if_dead(provider, "test-triggered recovery")
        .await;
    assert_eq!(
        builds.load(Ordering::SeqCst),
        1,
        "recovery must replace the dead source it proved dead, not be defeated by the reap"
    );
    drop(lease);
}

#[restate_core::test]
async fn recovery_is_scoped_to_the_provider_whose_mint_failed() {
    let provider_a =
        "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/aaaa";
    let provider_b =
        "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/bbbb";
    let service_account = "sa@example.iam.gserviceaccount.com";
    let audience_a = "https://federated-independent-a.example.com";
    let audience_b = "https://federated-independent-b.example.com";
    let fixture_a = recovery_fixture(
        RecoverySourceUnderTest::Federated(provider_a),
        audience_a,
        service_account,
        credential_source(ProbeOutcome::Dead),
    )
    .await;
    let provider_b_probes = Arc::new(AtomicUsize::new(0));
    let fixture_b = recovery_fixture(
        RecoverySourceUnderTest::Federated(provider_b),
        audience_b,
        service_account,
        healthy_source_with_probe_count(provider_b_probes.clone()),
    )
    .await;

    let outcome_b = mint_for_test(
        IdTokenIdentity::federated(provider_b, service_account),
        audience_b,
    )
    .await;
    assert_matches!(outcome_b, Err(GcpAuthError::Mint { .. }));
    wait_for_count(&provider_b_probes, 1).await;
    assert_eq!(
        fixture_b.rebuilds.load(Ordering::SeqCst),
        0,
        "provider_b's healthy source must not be replaced"
    );
    assert_eq!(
        fixture_a.rebuilds.load(Ordering::SeqCst),
        0,
        "a mint against provider_b must never rebuild provider_a's source"
    );

    let outcome_a = mint_for_test(
        IdTokenIdentity::federated(provider_a, service_account),
        audience_a,
    )
    .await;
    assert_matches!(outcome_a, Err(GcpAuthError::Mint { .. }));
    wait_for_count(&fixture_a.rebuilds, 1).await;
    assert_eq!(
        fixture_a.rebuilds.load(Ordering::SeqCst),
        1,
        "provider_a's dead source must be replaced exactly once"
    );
    assert_eq!(
        fixture_b.rebuilds.load(Ordering::SeqCst),
        0,
        "recovering provider_a's source must never touch provider_b's"
    );
}

#[restate_core::test]
async fn federated_provider_is_a_distinct_cache_key_dimension() {
    let audience = "https://wif-cache-key.example.com";
    let service_account = "sa@proj.iam.gserviceaccount.com";
    let provider =
        "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/r";

    let impersonated_key = IdTokenSpec::impersonated(audience, service_account);
    let federated_key = IdTokenSpec::federated(audience, provider, service_account);
    assert_ne!(impersonated_key, federated_key);

    let impersonated_builds = Arc::new(AtomicUsize::new(0));
    let federated_builds = Arc::new(AtomicUsize::new(0));

    add_build_override(impersonated_key, {
        let impersonated_builds = impersonated_builds.clone();
        move |_| {
            impersonated_builds.fetch_add(1, Ordering::SeqCst);
            Ok(MockSource::new(|_| MockOutcome::Token(token())) as Credential)
        }
    });
    add_build_override(federated_key, {
        let federated_builds = federated_builds.clone();
        move |_| {
            federated_builds.fetch_add(1, Ordering::SeqCst);
            Ok(MockSource::new(|_| MockOutcome::Token(token())) as Credential)
        }
    });

    for _ in 0..2 {
        mint_for_test(IdTokenIdentity::impersonated(service_account), audience)
            .await
            .expect("impersonated key mints");
        mint_for_test(
            IdTokenIdentity::federated(provider, service_account),
            audience,
        )
        .await
        .expect("federated key mints");
    }

    assert_eq!(
        impersonated_builds.load(Ordering::SeqCst),
        1,
        "the impersonated key must build exactly once, independently of the federated key"
    );
    assert_eq!(
        federated_builds.load(Ordering::SeqCst),
        1,
        "the federated key must build exactly once, independently of the impersonated key"
    );
}

#[restate_core::test]
async fn federation_requested_without_server_config_fails_closed_and_is_not_cached() {
    let provider =
        "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/r";
    let audience = "https://wif-no-config.example.com";
    let service_account = "sa@proj.iam.gserviceaccount.com";

    let err = mint_for_test(
        IdTokenIdentity::federated(provider, service_account),
        audience,
    )
    .await
    .expect_err("must fail without a [gcp-federation] configuration");
    assert!(
        err.to_string().contains("GCP source credentials"),
        "federated construction failures must not be mislabeled as ADC: {err}"
    );
    assert_matches!(err, GcpAuthError::CredentialSource { .. });

    let key = IdTokenSpec::federated(audience, provider, service_account);
    assert!(
        credential_registry().cache.get(&key).await.is_none(),
        "a construction failure must never populate the cache"
    );

    let err2 = mint_for_test(
        IdTokenIdentity::federated(provider, service_account),
        audience,
    )
    .await
    .expect_err("still fails without configuration");
    assert_matches!(err2, GcpAuthError::CredentialSource { .. });
}

/// Federated source recovery must run on the TaskCenter default runtime for the same reason
/// construction does: `google-cloud-auth` spawns its refresh task with a bare `tokio::spawn`,
/// so it must not land on a caller runtime that can be dropped.
#[test]
fn federated_source_recovery_runs_on_task_centers_default_runtime() {
    use restate_core::TaskCenterFutureExt as _;

    let provider = "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/recovery-runtime";
    assert_child_survives_caller_runtime(
        "federated source recovery",
        |task_center, refresh_task| {
            let registry = task_center.run_sync(credential_registry);
            let access_token_source = registry
                .federated_access_token_sources
                .get_or_create(provider);
            task_center.block_on(
                access_token_source
                    .credentials
                    .seed_for_test(credential_source(ProbeOutcome::Dead)),
            );
            federation::test_hooks::install_access_token_source_override(provider, move || {
                // Simulate the library refresh task spawned while rebuilding the source.
                refresh_task.spawn();
                Ok(credential_source(ProbeOutcome::Healthy))
            });
            (registry, access_token_source)
        },
        |caller_runtime, task_center, (registry, _source_lease)| {
            caller_runtime.block_on(
                registry
                    .federated_access_token_sources
                    .recover_if_dead(provider, "test-triggered recovery")
                    .in_tc(task_center),
            );
        },
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn blocking_builds_are_bounded_and_never_deadlock() {
    use std::sync::{Condvar, Mutex as StdMutex};

    let release = Arc::new((StdMutex::new(false), Condvar::new()));
    let (started_tx, mut started_rx) =
        tokio::sync::mpsc::channel(4 * MAX_CONCURRENT_BLOCKING_BUILDS);

    // Test the process-global semaphore's production choke point directly. This suite uses
    // nextest process isolation; construction overrides in the other tests bypass it.
    let tasks: Vec<_> = (0..4 * MAX_CONCURRENT_BLOCKING_BUILDS)
        .map(|_| {
            let release = release.clone();
            let started_tx = started_tx.clone();
            tokio::spawn(run_blocking("test".to_owned(), move || {
                started_tx.blocking_send(()).expect("receiver stays open");
                let (lock, wake) = &*release;
                let mut released = lock.lock().expect("release lock is not poisoned");
                while !*released {
                    released = wake.wait(released).expect("release lock is not poisoned");
                }
                Ok(ok_source())
            }))
        })
        .collect();

    for _ in 0..MAX_CONCURRENT_BLOCKING_BUILDS {
        started_rx.recv().await.expect("blocking build starts");
    }
    assert_eq!(BLOCKING_BUILD_PERMITS.available_permits(), 0);
    assert!(
        matches!(
            started_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "no additional blocking build may start while all permits are held"
    );

    let (lock, wake) = &*release;
    *lock.lock().expect("release lock is not poisoned") = true;
    wake.notify_all();
    tokio::time::timeout(Duration::from_secs(2), async {
        for task in tasks {
            let result = task.await.expect("task doesn't panic");
            if let Err(error) = &result {
                panic!("{error}");
            }
        }
    })
    .await
    .expect("blocking builds complete without deadlocking");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cancelled_callers_do_not_release_blocking_build_permits() {
    use std::sync::{Condvar, Mutex as StdMutex};

    let release = Arc::new((StdMutex::new(false), Condvar::new()));
    let (started_tx, mut started_rx) = tokio::sync::mpsc::channel(MAX_CONCURRENT_BLOCKING_BUILDS);

    let builds: Vec<_> = (0..MAX_CONCURRENT_BLOCKING_BUILDS)
        .map(|_| {
            let release = release.clone();
            let started_tx = started_tx.clone();
            tokio::spawn(spawn_bounded_blocking(move || {
                started_tx.blocking_send(()).expect("receiver stays open");
                let (lock, wake) = &*release;
                let mut released = lock.lock().expect("release lock is not poisoned");
                while !*released {
                    released = wake.wait(released).expect("release lock is not poisoned");
                }
            }))
        })
        .collect();
    for _ in 0..MAX_CONCURRENT_BLOCKING_BUILDS {
        started_rx.recv().await.expect("blocking build starts");
    }
    for build in builds {
        build.abort();
    }

    assert_eq!(
        BLOCKING_BUILD_PERMITS.available_permits(),
        0,
        "cancelled callers must retain permits until their blocking work finishes"
    );
    let replacement = tokio::spawn(spawn_bounded_blocking(|| {}));

    let (lock, wake) = &*release;
    *lock.lock().expect("release lock is not poisoned") = true;
    wake.notify_all();
    tokio::time::timeout(Duration::from_secs(5), replacement)
        .await
        .expect("replacement starts after blocking work exits")
        .expect("replacement task does not panic")
        .expect("blocking build does not panic");
}
