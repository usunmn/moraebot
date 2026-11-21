use crate::adapter::E_ADAPTER_EDIT;
use crate::adapter::{
    AdapterError, E_ADAPTER_DM, E_ADAPTER_SEND, E_ADAPTER_UPLOAD_DENIED, InboundAttachment,
    InboundEvent, MessengerAdapter, SendResult, TypingHandle,
};
use crate::bridge::client::{Connector, Link};
use crate::bridge::proto::{Caps, Decoded, Frame, FrameReader, ThreadsCap, write_frame};
use crate::bridge::{E_BRIDGE_UNSUPPORTED, PROTO_VERSION};
use crate::files::{E_FILE_WRITE, Progress};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, mpsc, watch};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Call {
    Send {
        channel: String,
        text: String,
    },
    CreateThread {
        channel_id: String,
        from_message_id: String,
        name: String,
    },
    StartTyping {
        channel: String,
    },
    OpenDm {
        user_id: String,
    },
    FetchAttachment {
        reference: String,
        dest: PathBuf,
    },
    SendFile {
        target: String,
        name: String,
        bytes: u64,
    },
    Edit {
        channel: String,
        message_id: String,
        text: String,
    },
}

/// The test's end of a streamed fetch: send chunks, read the running byte total, drop
/// `chunks` to finish the fetch.
pub struct FetchFeed {
    pub chunks: mpsc::Sender<Vec<u8>>,
    pub written: mpsc::UnboundedReceiver<u64>,
}

enum FetchMode {
    Hold(Arc<Notify>),
    Stream {
        chunks: mpsc::Receiver<Vec<u8>>,
        written: mpsc::UnboundedSender<u64>,
    },
}

#[derive(Default)]
struct FileScript {
    attachment_bytes: HashMap<String, Vec<u8>>,
    fetch_failures: VecDeque<&'static str>,
    send_failures: VecDeque<&'static str>,
    fetch_modes: VecDeque<FetchMode>,
    send_holds: VecDeque<Arc<Notify>>,
}

pub struct FakeAdapter {
    calls: Mutex<Vec<Call>>,
    call_count: watch::Sender<usize>,
    files: Mutex<FileScript>,
    sent_count: watch::Sender<usize>,
    fail_next_send: AtomicBool,
    fail_next_open_dm: AtomicBool,
    fail_next_edit: AtomicBool,
    reconnects: watch::Sender<u64>,
    inbound_tx: mpsc::Sender<InboundEvent>,
    inbound_rx: Mutex<Option<mpsc::Receiver<InboundEvent>>>,
}

impl Default for FakeAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeAdapter {
    pub fn new() -> Self {
        let (inbound_tx, inbound_rx) = mpsc::channel(64);
        Self {
            calls: Mutex::new(Vec::new()),
            call_count: watch::channel(0).0,
            files: Mutex::new(FileScript::default()),
            sent_count: watch::channel(0).0,
            fail_next_send: AtomicBool::new(false),
            fail_next_open_dm: AtomicBool::new(false),
            fail_next_edit: AtomicBool::new(false),
            reconnects: watch::channel(0).0,
            inbound_tx,
            inbound_rx: Mutex::new(Some(inbound_rx)),
        }
    }

    pub fn calls(&self) -> Vec<Call> {
        self.calls.lock().expect("fake adapter calls lock").clone()
    }

    pub fn fail_next_send(&self) {
        self.fail_next_send.store(true, Ordering::SeqCst);
    }

    /// The next `open_dm` fails with `E_ADAPTER_DM`.
    pub fn fail_next_open_dm(&self) {
        self.fail_next_open_dm.store(true, Ordering::SeqCst);
    }

    /// The next `edit` records its call, then fails with `E_ADAPTER_EDIT`.
    pub fn fail_next_edit(&self) {
        self.fail_next_edit.store(true, Ordering::SeqCst);
    }

    /// Simulates the messenger connection coming back (a ready or resume event).
    pub fn reconnect(&self) {
        self.reconnects.send_modify(|count| *count += 1);
    }

    pub async fn inject(&self, event: InboundEvent) {
        self.inbound_tx
            .send(event)
            .await
            .expect("inbound receiver was dropped");
    }

    /// The bytes `fetch_attachment` writes for an attachment with this reference.
    pub fn set_attachment_bytes(&self, reference: &str, bytes: Vec<u8>) {
        self.script()
            .attachment_bytes
            .insert(reference.to_owned(), bytes);
    }

    /// The next `fetch_attachment` fails with `code` (calls queue up).
    pub fn fail_next_fetch(&self, code: &'static str) {
        self.script().fetch_failures.push_back(code);
    }

    /// The next `send_file` fails with `code` after recording its call (calls queue up).
    pub fn fail_next_send_file(&self, code: &'static str) {
        self.script().send_failures.push_back(code);
    }

    /// The next `fetch_attachment` records its call, then waits for the returned `Notify`
    /// before returning its scripted result. It records no progress while it waits.
    pub fn hold_next_fetch(&self) -> Arc<Notify> {
        let gate = Arc::new(Notify::new());
        self.script()
            .fetch_modes
            .push_back(FetchMode::Hold(gate.clone()));
        gate
    }

    /// The next `fetch_attachment` writes every chunk the test sends, records progress per
    /// chunk and returns the total once the sender is dropped.
    pub fn stream_next_fetch(&self) -> FetchFeed {
        let (chunks_tx, chunks) = mpsc::channel(1);
        let (written, written_rx) = mpsc::unbounded_channel();
        self.script()
            .fetch_modes
            .push_back(FetchMode::Stream { chunks, written });
        FetchFeed {
            chunks: chunks_tx,
            written: written_rx,
        }
    }

    /// The next `send_file` records its call, then waits for the returned `Notify` and
    /// returns `Ok`. Never notified: it never completes.
    pub fn hold_next_send_file(&self) -> Arc<Notify> {
        let gate = Arc::new(Notify::new());
        self.script().send_holds.push_back(gate.clone());
        gate
    }

    /// Resolves once a recorded call satisfies `pred`. Wrap it in a timeout.
    pub async fn wait_for_call(&self, pred: impl Fn(&Call) -> bool) {
        let mut changed = self.call_count.subscribe();
        loop {
            if self.calls().iter().any(&pred) {
                return;
            }
            changed
                .changed()
                .await
                .expect("fake adapter owns the sender");
        }
    }

    fn script(&self) -> std::sync::MutexGuard<'_, FileScript> {
        self.files.lock().expect("fake adapter files lock")
    }

    fn record(&self, call: Call) {
        self.calls
            .lock()
            .expect("fake adapter calls lock")
            .push(call);
        self.call_count.send_modify(|count| *count += 1);
    }

    pub async fn wait_for_send(&self, n: usize) {
        self.sent_count
            .subscribe()
            .wait_for(|count| *count >= n)
            .await
            .map(|_| ())
            .expect("fake adapter owns the sender");
    }
}

#[async_trait]
impl MessengerAdapter for FakeAdapter {
    fn platform(&self) -> &'static str {
        "fake"
    }

    fn message_limit(&self) -> usize {
        2000
    }

    fn behavior_hints(&self) -> &'static str {
        "fake hints"
    }

    async fn start(&self) -> Result<mpsc::Receiver<InboundEvent>, AdapterError> {
        Ok(self
            .inbound_rx
            .lock()
            .expect("fake adapter receiver lock")
            .take()
            .expect("fake adapter started twice"))
    }

    async fn send(&self, channel: &str, text: &str) -> Result<SendResult, AdapterError> {
        let attempt = {
            self.record(Call::Send {
                channel: channel.to_owned(),
                text: text.to_owned(),
            });
            let attempt = self
                .calls()
                .iter()
                .filter(|call| matches!(call, Call::Send { .. }))
                .count();
            self.sent_count.send_replace(attempt);
            attempt
        };
        if self.fail_next_send.swap(false, Ordering::SeqCst) {
            return Err(AdapterError {
                code: E_ADAPTER_SEND,
                message: "injected send failure".to_owned(),
            });
        }
        Ok(SendResult {
            message_id: format!("fake-msg-{attempt}"),
        })
    }

    async fn create_thread(
        &self,
        channel_id: &str,
        from_message_id: &str,
        name: &str,
    ) -> Result<String, AdapterError> {
        self.record(Call::CreateThread {
            channel_id: channel_id.to_owned(),
            from_message_id: from_message_id.to_owned(),
            name: name.to_owned(),
        });
        Ok(from_message_id.to_owned())
    }

    async fn start_typing(&self, channel: &str) -> Result<TypingHandle, AdapterError> {
        self.record(Call::StartTyping {
            channel: channel.to_owned(),
        });
        Ok(TypingHandle::new(|| {}))
    }

    /// The DM target is `dm-<user_id>`.
    async fn open_dm(&self, user_id: &str) -> Result<String, AdapterError> {
        self.record(Call::OpenDm {
            user_id: user_id.to_owned(),
        });
        if self.fail_next_open_dm.swap(false, Ordering::SeqCst) {
            return Err(AdapterError {
                code: E_ADAPTER_DM,
                message: "injected open_dm failure".to_owned(),
            });
        }
        Ok(format!("dm-{user_id}"))
    }

    async fn fetch_attachment(
        &self,
        attachment: &InboundAttachment,
        dest: &Path,
        progress: &Progress,
    ) -> Result<u64, AdapterError> {
        self.record(Call::FetchAttachment {
            reference: attachment.reference.0.clone(),
            dest: dest.to_owned(),
        });
        let mode = self.script().fetch_modes.pop_front();
        if let Some(FetchMode::Stream {
            mut chunks,
            written,
        }) = mode
        {
            let mut file = create_private_file(dest)?;
            let mut total = 0u64;
            while let Some(chunk) = chunks.recv().await {
                if let Err(err) = progress.check_limit(total + chunk.len() as u64) {
                    drop(file);
                    let _ = std::fs::remove_file(dest);
                    return Err(err);
                }
                file.write_all(&chunk).map_err(|e| write_failure(&e))?;
                progress.record(chunk.len() as u64);
                total += chunk.len() as u64;
                let _ = written.send(total);
            }
            return Ok(total);
        }
        if let Some(FetchMode::Hold(gate)) = mode {
            gate.notified().await;
        }
        let (failure, bytes) = {
            let mut script = self.script();
            (
                script.fetch_failures.pop_front(),
                script
                    .attachment_bytes
                    .get(&attachment.reference.0)
                    .cloned(),
            )
        };
        if let Some(code) = failure {
            return Err(AdapterError {
                code,
                message: "injected fetch failure".to_owned(),
            });
        }
        let bytes = bytes.unwrap_or_default();
        progress.check_limit(bytes.len() as u64)?;
        create_private_file(dest)?
            .write_all(&bytes)
            .map_err(|e| write_failure(&e))?;
        progress.record(bytes.len() as u64);
        Ok(bytes.len() as u64)
    }

    async fn send_file(
        &self,
        target: &str,
        path: &Path,
        name: &str,
    ) -> Result<SendResult, AdapterError> {
        let data = tokio::fs::read(path).await;
        self.record(Call::SendFile {
            target: target.to_owned(),
            name: name.to_owned(),
            bytes: data.as_ref().map_or(0, |data| data.len() as u64),
        });
        let (failure, hold) = {
            let mut script = self.script();
            (
                script.send_failures.pop_front(),
                script.send_holds.pop_front(),
            )
        };
        if let Some(code) = failure {
            return Err(AdapterError {
                code,
                message: "injected send_file failure".to_owned(),
            });
        }
        if data.is_err() {
            return Err(AdapterError {
                code: E_ADAPTER_UPLOAD_DENIED,
                message: "the file could not be read".to_owned(),
            });
        }
        if let Some(gate) = hold {
            gate.notified().await;
        }
        Ok(SendResult {
            message_id: format!("fake-file-{name}"),
        })
    }

    async fn edit(&self, channel: &str, message_id: &str, text: &str) -> Result<(), AdapterError> {
        self.record(Call::Edit {
            channel: channel.to_owned(),
            message_id: message_id.to_owned(),
            text: text.to_owned(),
        });
        if self.fail_next_edit.swap(false, Ordering::SeqCst) {
            return Err(AdapterError {
                code: E_ADAPTER_EDIT,
                message: "injected edit failure".to_owned(),
            });
        }
        Ok(())
    }

    fn reconnects(&self) -> Option<watch::Receiver<u64>> {
        Some(self.reconnects.subscribe())
    }
}

/// A failed local write, like `files::fetch_to_file` reports it: the code and the error kind
/// only, never a path.
pub(crate) fn write_failure(err: &std::io::Error) -> AdapterError {
    AdapterError {
        code: E_FILE_WRITE,
        message: format!("could not write the file: {:?}", err.kind()),
    }
}

/// Creates `dest` like `files::fetch_to_file` does: new, 0600, parent must exist.
pub(crate) fn create_private_file(dest: &Path) -> Result<std::fs::File, AdapterError> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(dest)
        .map_err(|e| write_failure(&e))
}

/// The appended system prompt a fake-omp child read at start, from its log line
/// `START pid=<pid> prompt=<json> argv=<json>`: the contents of the file its
/// `--append-system-prompt` argument named when it started (a later start may have rewritten
/// the file since), empty when it had no such argument.
pub fn appended(start_line: &str) -> String {
    let (_, rest) = start_line
        .split_once(" prompt=")
        .expect("prompt field in START line");
    let (prompt, _) = rest.split_once(" argv=").expect("argv field in START line");
    serde_json::from_str::<Option<String>>(prompt)
        .expect("prompt JSON")
        .unwrap_or_default()
}

/// Starts a loopback HTTP server that accepts every connection and never answers; returns
/// its `http://` base URL. The server lives as long as the runtime.
pub async fn silent_http_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    format!("http://{addr}/")
}

/// How long a [`FakeBridge`] helper waits for the core before it panics.
const FAKE_BRIDGE_WAIT: Duration = Duration::from_secs(30);

/// Scripted stand-in for a bridge process, for `BridgeAdapter` tests. It is the
/// adapter's [`Connector`]: each connect hands the core one end of an in-memory stream
/// and queues the other end for [`FakeBridge::accept`]. While it is down, connects fail.
pub struct FakeBridge {
    up: AtomicBool,
    links: mpsc::UnboundedSender<tokio::io::DuplexStream>,
    accepted: tokio::sync::Mutex<mpsc::UnboundedReceiver<tokio::io::DuplexStream>>,
    attempts: watch::Sender<u64>,
}

impl FakeBridge {
    /// A bridge that accepts connections.
    pub fn new() -> Arc<Self> {
        let (links, accepted) = mpsc::unbounded_channel();
        Arc::new(Self {
            up: AtomicBool::new(true),
            links,
            accepted: tokio::sync::Mutex::new(accepted),
            attempts: watch::channel(0).0,
        })
    }

    /// `false`: every connect fails with "connection refused", as with no bridge running.
    pub fn set_up(&self, up: bool) {
        self.up.store(up, Ordering::SeqCst);
    }

    /// Counts connect attempts, failed ones included.
    pub fn attempts(&self) -> watch::Receiver<u64> {
        self.attempts.subscribe()
    }

    /// The bridge end of the next connection the core makes.
    pub async fn accept(&self) -> FakeBridgeConn {
        let link = tokio::time::timeout(FAKE_BRIDGE_WAIT, async {
            self.accepted.lock().await.recv().await
        })
        .await
        .expect("the core connected within the bound")
        .expect("the fake bridge is alive");
        let (read, writer) = tokio::io::split(link);
        FakeBridgeConn {
            frames: FrameReader::new(tokio::io::BufReader::new(read)),
            writer,
        }
    }

    /// A Slack-like hello: platform `slack`, limit 3900, home `C222`, every capability
    /// on and implicit threads.
    pub fn hello() -> Frame {
        Self::hello_with(Caps {
            threads: ThreadsCap::Implicit,
            dm: true,
            edit: true,
            files: true,
            status: true,
            reactions: false,
            agent_mode: true,
        })
    }

    /// [`Self::hello`] with other capabilities.
    pub fn hello_with(caps: Caps) -> Frame {
        Frame::Hello {
            proto: PROTO_VERSION,
            bridge: "fake-bridge".to_owned(),
            version: "0.0.0".to_owned(),
            platform: "slack".to_owned(),
            message_limit: 3900,
            hints: "fake bridge hints".to_owned(),
            home: Some("C222".to_owned()),
            caps,
            spooled: 0,
        }
    }
}

#[async_trait]
impl Connector for FakeBridge {
    async fn connect(&self) -> std::io::Result<Box<dyn Link>> {
        self.attempts.send_modify(|n| *n += 1);
        if !self.up.load(Ordering::SeqCst) {
            return Err(std::io::ErrorKind::ConnectionRefused.into());
        }
        let (core, bridge) = tokio::io::duplex(256 * 1024);
        self.links
            .send(bridge)
            .map_err(|_| std::io::Error::from(std::io::ErrorKind::ConnectionRefused))?;
        Ok(Box::new(core))
    }
}

/// The bridge end of one connection. Dropping it closes the link.
pub struct FakeBridgeConn {
    frames: FrameReader<tokio::io::BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
}

impl FakeBridgeConn {
    pub async fn send(&mut self, frame: &Frame) {
        write_frame(&mut self.writer, frame)
            .await
            .expect("the core end is open");
    }

    /// Writes `line` as it is, for malformed or unknown frames.
    pub async fn send_raw(&mut self, line: &[u8]) {
        use tokio::io::AsyncWriteExt;
        self.writer.write_all(line).await.expect("write");
        self.writer.flush().await.expect("flush");
    }

    /// The next frame the core wrote, pings included; `None` once the core closed the link.
    pub async fn next_frame(&mut self) -> Option<Frame> {
        let next = tokio::time::timeout(FAKE_BRIDGE_WAIT, self.frames.next())
            .await
            .expect("the core wrote within the bound");
        match next {
            Ok(Some(Decoded::Frame(frame))) => Some(frame),
            Ok(None) | Err(crate::bridge::proto::ProtoError::Io(_)) => None,
            other => panic!("the core wrote a bad frame: {other:?}"),
        }
    }

    /// The next frame that is not a core ping; pings are answered on the way.
    pub async fn recv(&mut self) -> Frame {
        loop {
            match self.next_frame().await {
                Some(Frame::Ping { n }) => self.send(&Frame::Pong { n }).await,
                Some(frame) => return frame,
                None => panic!("the core closed the link"),
            }
        }
    }

    /// Sends `hello` and returns the core's `hello_ok`.
    pub async fn handshake(&mut self, hello: &Frame) -> Frame {
        self.send(hello).await;
        let answer = self.recv().await;
        assert!(
            matches!(answer, Frame::HelloOk { .. }),
            "expected hello_ok, got {}",
            answer.kind()
        );
        answer
    }

    /// Asserts that the core wrote nothing but pings since the last read: pings the core
    /// and expects its pong as the next frame.
    pub async fn assert_quiet(&mut self) {
        const MARK: u64 = u64::MAX;
        self.send(&Frame::Ping { n: MARK }).await;
        let next = self.recv().await;
        assert_eq!(next, Frame::Pong { n: MARK }, "the core wrote a frame");
    }

    /// Waits until the core closes the link and returns the frames it wrote before,
    /// pings left out.
    pub async fn until_closed(&mut self) -> Vec<Frame> {
        let mut seen = Vec::new();
        while let Some(frame) = self.next_frame().await {
            if !matches!(frame, Frame::Ping { .. }) {
                seen.push(frame);
            }
        }
        seen
    }

    /// Hands the connection to a task that answers every request with `ok` (see
    /// [`AutoBridge`]).
    pub fn auto(self) -> AutoBridge {
        let record = Arc::new(Mutex::new(AutoRecord::default()));
        let changed = Arc::new(watch::channel(0u64).0);
        let (push, pushed) = mpsc::unbounded_channel();
        let task = tokio::spawn(auto_serve(
            self,
            Arc::clone(&record),
            Arc::clone(&changed),
            pushed,
        ));
        AutoBridge {
            record,
            changed,
            push,
            task,
        }
    }
}

/// What an [`AutoBridge`] saw: every core request and every acked `seq`, in order.
#[derive(Debug, Clone, Default)]
pub struct AutoRecord {
    pub requests: Vec<Frame>,
    pub acks: Vec<u64>,
}

impl AutoRecord {
    /// Every `send` as (target, text).
    pub fn sends(&self) -> Vec<(String, String)> {
        self.requests
            .iter()
            .filter_map(|frame| match frame {
                Frame::Send { target, text, .. } => Some((target.clone(), text.0.clone())),
                _ => None,
            })
            .collect()
    }
}

/// A connected fake bridge that answers on its own: `send` and `file` get a message
/// id, `thread` gets `from_message_id`, `dm` gets the user id, `edit` and `status` get
/// `ok`, `fetch` and `react` get `err`. Frames pushed with [`AutoBridge::push`] go to the
/// core. Dropping it closes the link.
pub struct AutoBridge {
    record: Arc<Mutex<AutoRecord>>,
    changed: Arc<watch::Sender<u64>>,
    push: mpsc::UnboundedSender<Frame>,
    task: tokio::task::JoinHandle<()>,
}

impl AutoBridge {
    pub fn push(&self, frame: Frame) {
        self.push.send(frame).expect("the auto bridge runs");
    }

    pub fn record(&self) -> AutoRecord {
        self.record.lock().expect("auto bridge record").clone()
    }

    /// Waits until `done` holds for the record.
    pub async fn wait_until(&self, done: impl Fn(&AutoRecord) -> bool) {
        let mut changed = self.changed.subscribe();
        tokio::time::timeout(FAKE_BRIDGE_WAIT, async {
            loop {
                if done(&self.record()) {
                    return;
                }
                changed.changed().await.expect("the auto bridge runs");
            }
        })
        .await
        .expect("the auto bridge saw it within the bound");
    }
}

impl Drop for AutoBridge {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// An `ok` answer to request `re`; set its fields with the `bridge_ok_*` helpers.
pub fn bridge_ok(re: u64) -> Frame {
    bridge_ok_fields(re, None, None, None, None)
}

/// An `ok` answer to request `re` carrying `message_id`.
pub fn bridge_ok_message(re: u64, message_id: impl Into<String>) -> Frame {
    bridge_ok_fields(re, Some(message_id.into()), None, None, None)
}

/// An `ok` answer to request `re` carrying `thread_id`.
pub fn bridge_ok_thread(re: u64, thread_id: impl Into<String>) -> Frame {
    bridge_ok_fields(re, None, Some(thread_id.into()), None, None)
}

/// An `ok` answer to request `re` carrying `target`.
pub fn bridge_ok_target(re: u64, target: impl Into<String>) -> Frame {
    bridge_ok_fields(re, None, None, Some(target.into()), None)
}

/// An `ok` answer to request `re` carrying `bytes`.
pub fn bridge_ok_bytes(re: u64, bytes: u64) -> Frame {
    bridge_ok_fields(re, None, None, None, Some(bytes))
}

fn bridge_ok_fields(
    re: u64,
    message_id: Option<String>,
    thread_id: Option<String>,
    target: Option<String>,
    bytes: Option<u64>,
) -> Frame {
    Frame::Ok {
        re,
        message_id,
        thread_id,
        target,
        bytes,
    }
}

/// Aborts the reader task of an [`AutoBridge`] with it, so the link closes.
struct AbortReader(tokio::task::JoinHandle<()>);

impl Drop for AbortReader {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn auto_serve(
    conn: FakeBridgeConn,
    record: Arc<Mutex<AutoRecord>>,
    changed: Arc<watch::Sender<u64>>,
    mut pushed: mpsc::UnboundedReceiver<Frame>,
) {
    let FakeBridgeConn {
        mut frames,
        mut writer,
    } = conn;
    // FrameReader::next is not cancel safe, so it runs on its own task.
    let (read_tx, mut read) = mpsc::unbounded_channel();
    let _reader = AbortReader(tokio::spawn(async move {
        while let Ok(Some(Decoded::Frame(frame))) = frames.next().await {
            if read_tx.send(frame).is_err() {
                return;
            }
        }
    }));
    let note = |change: &dyn Fn(&mut AutoRecord)| {
        change(&mut record.lock().expect("auto bridge record"));
        changed.send_modify(|n| *n += 1);
    };
    loop {
        let frame = tokio::select! {
            frame = read.recv() => match frame {
                Some(frame) => frame,
                None => return,
            },
            Some(frame) = pushed.recv() => {
                if write_frame(&mut writer, &frame).await.is_err() {
                    return;
                }
                continue;
            }
        };
        let answer = match &frame {
            Frame::Ping { n } => Some(Frame::Pong { n: *n }),
            Frame::Ack { seq } => {
                let seq = *seq;
                note(&|r| r.acks.push(seq));
                None
            }
            Frame::Send { id, target, .. } => {
                Some(bridge_ok_message(*id, format!("{target}:auto-{id}")))
            }
            Frame::File { id, .. } => Some(bridge_ok_message(*id, format!("F{id}"))),
            Frame::Thread {
                id,
                from_message_id,
                ..
            } => Some(bridge_ok_thread(*id, from_message_id.clone())),
            Frame::Dm { id, user_id } => Some(bridge_ok_target(*id, user_id.clone())),
            Frame::Edit { id, .. } | Frame::Status { id, .. } => Some(bridge_ok(*id)),
            Frame::Fetch { id, .. } | Frame::React { id, .. } => Some(Frame::Err {
                re: *id,
                code: E_BRIDGE_UNSUPPORTED.to_owned(),
                message: "the auto bridge serves no downloads or reactions".to_owned(),
            }),
            _ => None,
        };
        if frame.request_id().is_some() {
            let seen = frame.clone();
            note(&|r| r.requests.push(seen.clone()));
        }
        if let Some(answer) = answer
            && write_frame(&mut writer, &answer).await.is_err()
        {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::AttachmentRef;
    use crate::adapter::E_ADAPTER_FILES;
    use std::os::unix::fs::PermissionsExt;

    fn attachment(reference: &str) -> InboundAttachment {
        InboundAttachment {
            name: "a.bin".into(),
            size: 0,
            mime: None,
            reference: AttachmentRef(reference.into()),
        }
    }

    #[tokio::test]
    async fn fake_open_dm_records_and_can_fail() {
        let fake = FakeAdapter::new();
        assert_eq!(fake.open_dm("111").await, Ok("dm-111".to_owned()));
        fake.fail_next_open_dm();
        assert_eq!(fake.open_dm("111").await.unwrap_err().code, E_ADAPTER_DM);
        assert_eq!(fake.open_dm("111").await, Ok("dm-111".to_owned()));
        let open = Call::OpenDm {
            user_id: "111".into(),
        };
        assert_eq!(fake.calls(), vec![open.clone(), open.clone(), open]);
    }

    #[tokio::test]
    async fn fake_adapter_fetch_records_and_reports_progress() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("a.bin");
        let fake = FakeAdapter::new();
        fake.set_attachment_bytes("ref-1", vec![7; 5]);
        let progress = Progress::new();
        let n = fake
            .fetch_attachment(&attachment("ref-1"), &dest, &progress)
            .await
            .unwrap();
        assert_eq!(n, 5);
        assert_eq!(progress.bytes(), 5);
        assert_eq!(std::fs::read(&dest).unwrap(), vec![7; 5]);
        assert_eq!(
            std::fs::metadata(&dest).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            fake.calls(),
            vec![Call::FetchAttachment {
                reference: "ref-1".into(),
                dest
            }]
        );
    }

    #[tokio::test]
    async fn fake_adapter_streamed_fetch_writes_each_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("s.bin");
        let fake = Arc::new(FakeAdapter::new());
        let mut feed = fake.stream_next_fetch();
        let task = {
            let fake = fake.clone();
            let dest = dest.clone();
            tokio::spawn(async move {
                let progress = Progress::new();
                fake.fetch_attachment(&attachment("s"), &dest, &progress)
                    .await
            })
        };
        let limit = Duration::from_secs(30);
        feed.chunks.send(b"abc".to_vec()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(limit, feed.written.recv())
                .await
                .unwrap(),
            Some(3)
        );
        feed.chunks.send(b"def".to_vec()).await.unwrap();
        assert_eq!(
            tokio::time::timeout(limit, feed.written.recv())
                .await
                .unwrap(),
            Some(6)
        );
        drop(feed.chunks);
        let result = tokio::time::timeout(limit, task).await.unwrap().unwrap();
        assert_eq!(result.unwrap(), 6);
        assert_eq!(std::fs::read(&dest).unwrap(), b"abcdef");
    }

    #[tokio::test]
    async fn fake_adapter_held_fetch_waits_for_notify() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("h.bin");
        let fake = Arc::new(FakeAdapter::new());
        fake.set_attachment_bytes("h", vec![1, 2]);
        let gate = fake.hold_next_fetch();
        let task = {
            let fake = fake.clone();
            let dest = dest.clone();
            tokio::spawn(async move {
                fake.fetch_attachment(&attachment("h"), &dest, &Progress::new())
                    .await
            })
        };
        let limit = Duration::from_secs(30);
        tokio::time::timeout(
            limit,
            fake.wait_for_call(|c| matches!(c, Call::FetchAttachment { .. })),
        )
        .await
        .unwrap();
        assert!(
            !dest.exists(),
            "a held fetch writes nothing before the notify"
        );
        assert!(!task.is_finished());
        gate.notify_one();
        let n = tokio::time::timeout(limit, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(n, 2);
        assert!(dest.exists());
    }

    #[tokio::test]
    async fn fake_adapter_fetch_without_parent_dir_is_write_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("no-such-dir");
        let fake = FakeAdapter::new();
        let err = fake
            .fetch_attachment(&attachment("x"), &missing.join("f.bin"), &Progress::new())
            .await
            .unwrap_err();
        assert_eq!(err.code, E_FILE_WRITE);
        assert!(!missing.exists(), "the fake never creates directories");
    }

    #[tokio::test]
    async fn fake_adapter_send_file_unreadable_is_denied() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("secret.bin");
        std::fs::write(&path, b"x").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
        let fake = FakeAdapter::new();
        let err = fake.send_file("t", &path, "secret.bin").await.unwrap_err();
        assert_eq!(err.code, E_ADAPTER_UPLOAD_DENIED);
    }

    #[tokio::test]
    async fn fake_adapter_held_send_file_waits_for_notify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        std::fs::write(&path, b"xyz").unwrap();
        let fake = Arc::new(FakeAdapter::new());
        let gate = fake.hold_next_send_file();
        let task = {
            let fake = fake.clone();
            let path = path.clone();
            tokio::spawn(async move { fake.send_file("t", &path, "f.bin").await })
        };
        let limit = Duration::from_secs(30);
        tokio::time::timeout(
            limit,
            fake.wait_for_call(|c| matches!(c, Call::SendFile { .. })),
        )
        .await
        .unwrap();
        assert!(!task.is_finished());
        gate.notify_one();
        tokio::time::timeout(limit, task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn fake_adapter_send_file_reads_whole_file_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big.bin");
        std::fs::write(&path, vec![9u8; 200 * 1024]).unwrap();
        let fake = FakeAdapter::new();
        fake.send_file("thread-1", &path, "big.bin").await.unwrap();
        assert_eq!(
            fake.calls(),
            vec![Call::SendFile {
                target: "thread-1".into(),
                name: "big.bin".into(),
                bytes: 200 * 1024
            }]
        );
    }

    #[tokio::test]
    async fn fake_adapter_injected_file_failures_return_their_code() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin");
        std::fs::write(&path, b"x").unwrap();
        let fake = FakeAdapter::new();
        fake.fail_next_fetch(E_ADAPTER_FILES);
        fake.fail_next_send_file(E_ADAPTER_FILES);
        let fetch = fake
            .fetch_attachment(&attachment("x"), &dir.path().join("o"), &Progress::new())
            .await
            .unwrap_err();
        assert_eq!(fetch.code, E_ADAPTER_FILES);
        let send = fake.send_file("t", &path, "f.bin").await.unwrap_err();
        assert_eq!(send.code, E_ADAPTER_FILES);
        assert_eq!(fake.calls().len(), 2, "failing calls are recorded");
    }
}
