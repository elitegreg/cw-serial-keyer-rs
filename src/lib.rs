//! Async interface for keying Morse/CW on serial port control lines.
//!
//! The public API is intentionally close to `winkeyer-rs`: open a device, set
//! WPM/weighting, queue text with [`SerialKeyer::send_text`], wait for idle, and
//! close.  Unlike a WinKeyer, Morse timing is generated locally in a dedicated
//! blocking thread so Tokio scheduling jitter does not key the transmitter.
//!
//! cwdaemon uses libcw to convert text to Morse and a serial driver that toggles
//! DTR or RTS with `TIOCMBIS`/`TIOCMBIC`.  This crate implements the same core
//! behavior directly: one dit is `1200 / wpm` ms, dashes are three dits, gaps are
//! one dit within a character, three dits between characters, and seven dits
//! between words.  Sidetone, UDP, and cwdaemon escape commands are intentionally
//! omitted.

use std::collections::VecDeque;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

pub mod tcp_proxy;
pub use tcp_proxy::SerialTcpServer;

use serialport::SerialPort;
use thiserror::Error;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::time::{sleep, timeout};

const DEFAULT_WPM: u8 = 24;
const DEFAULT_WEIGHTING: u8 = 50;
const DEFAULT_POLL_DELAY: Duration = Duration::from_millis(20);
const DEFAULT_TIMEOUT: Duration = Duration::from_millis(500);
const SERIAL_STREAM_BUFFER_SIZE: usize = 4096;

#[derive(Debug, Error)]
pub enum Error {
    #[error("serial port error: {0}")]
    Serial(#[from] serialport::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("keyer worker thread stopped")]
    Closed,
    #[error("operation timed out")]
    Timeout,
    #[error("WPM must be in the range 5..=99, got {0}")]
    InvalidWpm(u8),
    #[error("weighting must be in the range 10..=90, got {0}")]
    InvalidWeighting(u8),
    #[error("key and PTT lines must be different when both are enabled")]
    ConflictingLines,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Serial modem-control line used for output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlLine {
    Dtr,
    Rts,
}

/// Open-time serial keyer configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub path: PathBuf,
    pub baud_rate: u32,
    pub key_line: ControlLine,
    pub ptt_line: Option<ControlLine>,
    pub wpm: u8,
    pub weighting: u8,
    pub ptt_lead: Duration,
    pub ptt_tail: Duration,
}

impl Config {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            baud_rate: 9600,
            key_line: ControlLine::Dtr,
            ptt_line: None,
            wpm: DEFAULT_WPM,
            weighting: DEFAULT_WEIGHTING,
            ptt_lead: Duration::ZERO,
            ptt_tail: Duration::ZERO,
        }
    }

    pub fn baud_rate(mut self, baud_rate: u32) -> Self {
        self.baud_rate = baud_rate;
        self
    }

    pub fn key_line(mut self, line: ControlLine) -> Self {
        self.key_line = line;
        self
    }
    pub fn ptt_line(mut self, line: Option<ControlLine>) -> Self {
        self.ptt_line = line;
        self
    }
    pub fn wpm(mut self, wpm: u8) -> Self {
        self.wpm = wpm;
        self
    }
    pub fn weighting(mut self, weighting: u8) -> Self {
        self.weighting = weighting;
        self
    }
    pub fn ptt_lead_tail(mut self, lead: Duration, tail: Duration) -> Self {
        self.ptt_lead = lead;
        self.ptt_tail = tail;
        self
    }
}

/// Snapshot of the software keyer state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    pub key_down: bool,
    pub ptt_on: bool,
    pub busy: bool,
    pub queued_messages: usize,
}

/// Bidirectional raw serial byte channel backed by the keyer worker.
pub struct SerialChannel {
    pub(crate) tx: mpsc::Sender<Command>,
    pub(crate) read_tx: broadcast::Sender<Vec<u8>>,
    rx: broadcast::Receiver<Vec<u8>>,
}

impl SerialChannel {
    pub(crate) fn resubscribe(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            read_tx: self.read_tx.clone(),
            rx: self.read_tx.subscribe(),
        }
    }

    /// Write raw bytes to the serial port.
    pub async fn write(&self, bytes: impl AsRef<[u8]>) -> Result<()> {
        write_serial_command(&self.tx, bytes.as_ref().to_vec()).await
    }

    /// Read the next chunk of bytes received from the serial port.
    pub async fn read(&mut self) -> Result<Vec<u8>> {
        loop {
            match self.rx.recv().await {
                Ok(bytes) => return Ok(bytes),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return Err(Error::Closed),
            }
        }
    }
}

/// Async handle for a background serial-line Morse keyer.
pub struct SerialKeyer {
    tx: mpsc::Sender<Command>,
    serial_rx: broadcast::Sender<Vec<u8>>,
    timeout: Duration,
    worker: Option<thread::JoinHandle<()>>,
}

impl SerialKeyer {
    /// Open a serial device using DTR for CW keying and no PTT line.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(Config::new(path)).await
    }

    /// Open a serial device with explicit key/PTT line configuration.
    pub async fn open_with_config(config: Config) -> Result<Self> {
        validate_config(&config)?;
        let (tx, rx) = mpsc::channel();
        let (serial_rx, _serial_read_rx) = broadcast::channel(64);
        let serial_read_tx = serial_rx.clone();
        let (ready_tx, ready_rx) = oneshot::channel();
        let worker = thread::spawn(move || worker_main(config, rx, ready_tx, serial_read_tx));
        ready_rx.await.map_err(|_| Error::Closed)??;
        Ok(Self {
            tx,
            serial_rx,
            timeout: DEFAULT_TIMEOUT,
            worker: Some(worker),
        })
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
    }

    /// Queue text for transmission. Returns after the text has been accepted.
    pub async fn send_text(&mut self, text: impl AsRef<str>) -> Result<()> {
        self.call_ack(CommandKind::SendText(text.as_ref().to_string()))
            .await
    }

    /// Abort current sending and clear queued text. Lines are returned off.
    pub async fn clear_buffer(&mut self) -> Result<()> {
        self.call_ack(CommandKind::ClearBuffer).await
    }

    pub async fn set_wpm(&mut self, wpm: u8) -> Result<()> {
        if !(5..=99).contains(&wpm) {
            return Err(Error::InvalidWpm(wpm));
        }
        self.call_ack(CommandKind::SetWpm(wpm)).await
    }

    /// Set weighting as a keyer-style percentage. 50 is nominal.
    pub async fn set_weighting(&mut self, weighting: u8) -> Result<()> {
        if !(10..=90).contains(&weighting) {
            return Err(Error::InvalidWeighting(weighting));
        }
        self.call_ack(CommandKind::SetWeighting(weighting)).await
    }

    pub async fn set_ptt_lead_tail(&mut self, lead: Duration, tail: Duration) -> Result<()> {
        self.call_ack(CommandKind::SetPttLeadTail { lead, tail })
            .await
    }

    pub async fn status(&mut self) -> Result<Status> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(Command {
                kind: CommandKind::Status(reply_tx),
                ack: None,
            })
            .map_err(|_| Error::Closed)?;
        timeout(self.timeout, reply_rx)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Closed)
    }

    pub async fn wait_until_idle(&mut self) -> Result<()> {
        loop {
            if !self.status().await?.busy {
                return Ok(());
            }
            sleep(DEFAULT_POLL_DELAY).await;
        }
    }

    /// Create a bidirectional raw serial byte channel.
    pub fn serial_channel(&self) -> SerialChannel {
        SerialChannel {
            tx: self.tx.clone(),
            read_tx: self.serial_rx.clone(),
            rx: self.serial_rx.subscribe(),
        }
    }

    /// Create a Tokio byte stream backed by the same serial port used for keying.
    ///
    /// Bytes written to the returned stream are written to the serial port through
    /// the keyer worker. Bytes read by the keyer worker are readable from the
    /// stream. Dropping the stream stops the bridge task.
    pub fn serial_stream(&self) -> DuplexStream {
        serial_stream_from_channel(self.serial_channel())
    }

    /// Write raw bytes to the serial port through the keyer worker.
    pub async fn write_serial(&self, bytes: impl AsRef<[u8]>) -> Result<()> {
        write_serial_command(&self.tx, bytes.as_ref().to_vec()).await
    }

    /// Subscribe to raw bytes read from the serial port.
    pub fn subscribe_serial(&self) -> broadcast::Receiver<Vec<u8>> {
        self.serial_rx.subscribe()
    }

    /// Start a Tokio TCP server that proxies bytes between TCP clients and the
    /// same serial port used for keying.
    pub async fn start_tcp_server(
        &self,
        addr: impl tokio::net::ToSocketAddrs,
    ) -> Result<SerialTcpServer> {
        SerialTcpServer::start(addr, self.serial_channel()).await
    }

    pub async fn close(&mut self) -> Result<()> {
        let result = self.call_ack(CommandKind::Close).await;
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
        result
    }

    async fn call_ack(&mut self, kind: CommandKind) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(Command {
                kind,
                ack: Some(ack_tx),
            })
            .map_err(|_| Error::Closed)?;
        timeout(self.timeout, ack_rx)
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(|_| Error::Closed)?
    }
}

impl Drop for SerialKeyer {
    fn drop(&mut self) {
        let _ = self.tx.send(Command {
            kind: CommandKind::Close,
            ack: None,
        });
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn serial_stream_from_channel(mut channel: SerialChannel) -> DuplexStream {
    let (stream, mut bridge_stream) = duplex(SERIAL_STREAM_BUFFER_SIZE);

    tokio::spawn(async move {
        let mut stream_buf = [0_u8; SERIAL_STREAM_BUFFER_SIZE];

        loop {
            tokio::select! {
                read = bridge_stream.read(&mut stream_buf) => {
                    let Ok(read) = read else {
                        break;
                    };
                    if read == 0 {
                        break;
                    }
                    if channel.write(&stream_buf[..read]).await.is_err() {
                        break;
                    }
                }
                serial = channel.read() => {
                    let Ok(bytes) = serial else {
                        break;
                    };
                    if bridge_stream.write_all(&bytes).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    stream
}

pub(crate) async fn write_serial_command(tx: &mpsc::Sender<Command>, bytes: Vec<u8>) -> Result<()> {
    let (ack_tx, ack_rx) = oneshot::channel();
    tx.send(Command {
        kind: CommandKind::WriteSerial(bytes),
        ack: Some(ack_tx),
    })
    .map_err(|_| Error::Closed)?;
    ack_rx.await.map_err(|_| Error::Closed)??;
    Ok(())
}

pub(crate) struct Command {
    pub(crate) kind: CommandKind,
    pub(crate) ack: Option<oneshot::Sender<Result<()>>>,
}

pub(crate) enum CommandKind {
    SendText(String),
    ClearBuffer,
    SetWpm(u8),
    SetWeighting(u8),
    SetPttLeadTail { lead: Duration, tail: Duration },
    Status(oneshot::Sender<Status>),
    WriteSerial(Vec<u8>),
    Close,
}

struct Worker {
    port: Box<dyn SerialPort>,
    key_line: ControlLine,
    ptt_line: Option<ControlLine>,
    wpm: u8,
    weighting: u8,
    ptt_lead: Duration,
    ptt_tail: Duration,
    queue: VecDeque<String>,
    key_down: bool,
    ptt_on: bool,
    closing: bool,
    serial_read_tx: broadcast::Sender<Vec<u8>>,
}

fn worker_main(
    config: Config,
    rx: mpsc::Receiver<Command>,
    ready: oneshot::Sender<Result<()>>,
    serial_read_tx: broadcast::Sender<Vec<u8>>,
) {
    let mut port = match serialport::new(config.path.to_string_lossy(), config.baud_rate)
        .timeout(Duration::from_millis(100))
        .dtr_on_open(false)
        .flow_control(serialport::FlowControl::None)
        .open()
    {
        Ok(port) => port,
        Err(err) => {
            let _ = ready.send(Err(err.into()));
            return;
        }
    };

    if let Err(err) = set_line(&mut *port, config.key_line, false).and_then(|_| {
        if let Some(line) = config.ptt_line {
            set_line(&mut *port, line, false)
        } else {
            Ok(())
        }
    }) {
        let _ = ready.send(Err(err));
        return;
    }

    let mut worker = Worker {
        port,
        key_line: config.key_line,
        ptt_line: config.ptt_line,
        wpm: config.wpm,
        weighting: config.weighting,
        ptt_lead: config.ptt_lead,
        ptt_tail: config.ptt_tail,
        queue: VecDeque::new(),
        key_down: false,
        ptt_on: false,
        closing: false,
        serial_read_tx,
    };
    let _ = ready.send(Ok(()));

    while !worker.closing {
        let _ = worker.read_serial_available();
        match worker.queue.pop_front() {
            Some(text) => {
                let _ = send_message(&mut worker, &rx, &text);
            }
            None => match rx.recv_timeout(DEFAULT_POLL_DELAY) {
                Ok(cmd) => handle_command(&mut worker, cmd),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => worker.closing = true,
            },
        }
    }
    let _ = worker.set_key(false);
    let _ = worker.set_ptt(false);
}

fn handle_command(worker: &mut Worker, cmd: Command) {
    let result = match cmd.kind {
        CommandKind::SendText(text) => {
            worker.queue.push_back(text);
            Ok(())
        }
        CommandKind::ClearBuffer => {
            worker.queue.clear();
            let _ = worker.set_key(false);
            let _ = worker.set_ptt(false);
            Ok(())
        }
        CommandKind::SetWpm(wpm) => {
            worker.wpm = wpm;
            Ok(())
        }
        CommandKind::SetWeighting(weighting) => {
            worker.weighting = weighting;
            Ok(())
        }
        CommandKind::SetPttLeadTail { lead, tail } => {
            worker.ptt_lead = lead;
            worker.ptt_tail = tail;
            Ok(())
        }
        CommandKind::Status(reply) => {
            let _ = reply.send(worker.status());
            Ok(())
        }
        CommandKind::WriteSerial(bytes) => worker.port.write_all(&bytes).map_err(Into::into),
        CommandKind::Close => {
            worker.closing = true;
            Ok(())
        }
    };
    if let Some(ack) = cmd.ack {
        let _ = ack.send(result);
    }
}

fn send_message(worker: &mut Worker, rx: &mpsc::Receiver<Command>, text: &str) -> Result<()> {
    if !text.trim().is_empty() {
        worker.set_ptt(true)?;
        worker_sleep(worker, rx, worker.ptt_lead);
    }

    let mut need_char_gap = false;
    for token in tokenize(text) {
        drain_commands(worker, rx);
        if worker.closing {
            break;
        }
        match token {
            Token::WordGap => {
                worker_sleep(worker, rx, worker.unit() * 7);
                need_char_gap = false;
            }
            Token::Char(pattern) => {
                if need_char_gap {
                    worker_sleep(worker, rx, worker.unit() * 3);
                }
                send_pattern(worker, rx, pattern)?;
                need_char_gap = true;
            }
        }
    }

    if worker.queue.is_empty() {
        worker_sleep(worker, rx, worker.ptt_tail);
        worker.set_ptt(false)?;
    }
    Ok(())
}

fn drain_commands(worker: &mut Worker, rx: &mpsc::Receiver<Command>) {
    let _ = worker.read_serial_available();
    while let Ok(cmd) = rx.try_recv() {
        handle_command(worker, cmd);
        let _ = worker.read_serial_available();
    }
}

fn worker_sleep(worker: &mut Worker, rx: &mpsc::Receiver<Command>, duration: Duration) {
    if duration.is_zero() {
        return;
    }
    let deadline = Instant::now() + duration;
    while !worker.closing {
        let _ = worker.read_serial_available();
        drain_commands(worker, rx);
        if Instant::now() >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        thread::sleep(remaining.min(DEFAULT_POLL_DELAY));
    }
}

fn send_pattern(worker: &mut Worker, rx: &mpsc::Receiver<Command>, pattern: &str) -> Result<()> {
    let symbols: Vec<char> = pattern.chars().collect();
    for (idx, symbol) in symbols.iter().enumerate() {
        let nominal = match symbol {
            '.' => worker.unit(),
            '-' => worker.unit() * 3,
            _ => Duration::ZERO,
        };
        worker.set_key(true)?;
        worker_sleep(worker, rx, worker.mark_duration(nominal));
        worker.set_key(false)?;
        if idx + 1 < symbols.len() {
            worker_sleep(worker, rx, worker.space_duration(worker.unit()));
        }
    }
    Ok(())
}

impl Worker {
    fn unit(&self) -> Duration {
        Duration::from_secs_f64(1.2 / f64::from(self.wpm))
    }

    fn mark_duration(&self, nominal: Duration) -> Duration {
        nominal.mul_f64(f64::from(self.weighting) / 50.0)
    }

    fn space_duration(&self, nominal: Duration) -> Duration {
        // Keep dit+following intra-element space close to two units while the
        // mark changes with weighting. Clamp to zero for very heavy weighting.
        let weighted_dit = self.mark_duration(self.unit());
        nominal
            .saturating_add(self.unit())
            .saturating_sub(weighted_dit)
    }

    fn set_key(&mut self, on: bool) -> Result<()> {
        set_line(&mut *self.port, self.key_line, on)?;
        self.key_down = on;
        Ok(())
    }

    fn set_ptt(&mut self, on: bool) -> Result<()> {
        if let Some(line) = self.ptt_line {
            set_line(&mut *self.port, line, on)?;
        }
        self.ptt_on = on;
        Ok(())
    }

    fn read_serial_available(&mut self) -> Result<()> {
        let available = match self.port.bytes_to_read() {
            Ok(0) => return Ok(()),
            Ok(n) => n as usize,
            Err(err) => return Err(err.into()),
        };
        let mut buf = vec![0_u8; available.min(4096)];
        match self.port.read(&mut buf) {
            Ok(0) => Ok(()),
            Ok(n) => {
                buf.truncate(n);
                let _ = self.serial_read_tx.send(buf);
                Ok(())
            }
            Err(err)
                if err.kind() == ErrorKind::WouldBlock || err.kind() == ErrorKind::TimedOut =>
            {
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    fn status(&self) -> Status {
        Status {
            key_down: self.key_down,
            ptt_on: self.ptt_on,
            busy: self.key_down || self.ptt_on || !self.queue.is_empty(),
            queued_messages: self.queue.len(),
        }
    }
}

fn set_line(port: &mut dyn SerialPort, line: ControlLine, on: bool) -> Result<()> {
    match line {
        ControlLine::Dtr => port.write_data_terminal_ready(on).map_err(Into::into),
        ControlLine::Rts => port.write_request_to_send(on).map_err(Into::into),
    }
}

#[derive(Clone, Copy)]
enum Token {
    Char(&'static str),
    WordGap,
}

fn tokenize(text: &str) -> impl Iterator<Item = Token> + '_ {
    let mut last_was_space = true;
    text.chars().filter_map(move |ch| {
        if ch.is_whitespace() {
            if last_was_space {
                None
            } else {
                last_was_space = true;
                Some(Token::WordGap)
            }
        } else {
            last_was_space = false;
            morse(ch).map(Token::Char)
        }
    })
}

fn morse(ch: char) -> Option<&'static str> {
    match ch.to_ascii_uppercase() {
        'A' => Some(".-"),
        'B' => Some("-..."),
        'C' => Some("-.-."),
        'D' => Some("-.."),
        'E' => Some("."),
        'F' => Some("..-."),
        'G' => Some("--."),
        'H' => Some("...."),
        'I' => Some(".."),
        'J' => Some(".---"),
        'K' => Some("-.-"),
        'L' => Some(".-.."),
        'M' => Some("--"),
        'N' => Some("-."),
        'O' => Some("---"),
        'P' => Some(".--."),
        'Q' => Some("--.-"),
        'R' => Some(".-."),
        'S' => Some("..."),
        'T' => Some("-"),
        'U' => Some("..-"),
        'V' => Some("...-"),
        'W' => Some(".--"),
        'X' => Some("-..-"),
        'Y' => Some("-.--"),
        'Z' => Some("--.."),
        '0' => Some("-----"),
        '1' => Some(".----"),
        '2' => Some("..---"),
        '3' => Some("...--"),
        '4' => Some("....-"),
        '5' => Some("....."),
        '6' => Some("-...."),
        '7' => Some("--..."),
        '8' => Some("---.."),
        '9' => Some("----."),
        '.' => Some(".-.-.-"),
        ',' => Some("--..--"),
        '?' => Some("..--.."),
        '\'' => Some(".----."),
        '!' => Some("-.-.--"),
        '/' => Some("-..-."),
        '(' => Some("-.--."),
        ')' => Some("-.--.-"),
        '&' => Some(".-..."),
        ':' => Some("---..."),
        ';' => Some("-.-.-."),
        '=' => Some("-...-"),
        '+' => Some(".-.-."),
        '-' => Some("-....-"),
        '_' => Some("..--.-"),
        '"' => Some(".-..-."),
        '$' => Some("...-..-"),
        '@' => Some(".--.-."),
        _ => None,
    }
}

fn validate_config(config: &Config) -> Result<()> {
    if let Some(ptt) = config.ptt_line {
        if ptt == config.key_line {
            return Err(Error::ConflictingLines);
        }
    }
    if !(5..=99).contains(&config.wpm) {
        return Err(Error::InvalidWpm(config.wpm));
    }
    if !(10..=90).contains(&config.weighting) {
        return Err(Error::InvalidWeighting(config.weighting));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_serial_channel() -> (
        SerialChannel,
        mpsc::Receiver<Command>,
        broadcast::Sender<Vec<u8>>,
    ) {
        let (tx, rx) = mpsc::channel();
        let (read_tx, _) = broadcast::channel(16);
        let channel = SerialChannel {
            tx,
            read_tx: read_tx.clone(),
            rx: read_tx.subscribe(),
        };
        (channel, rx, read_tx)
    }

    #[test]
    fn maps_common_morse() {
        assert_eq!(morse('s'), Some("..."));
        assert_eq!(morse('O'), Some("---"));
        assert_eq!(morse('?'), Some("..--.."));
    }

    #[test]
    fn tokenizes_single_word_gap() {
        let tokens: Vec<_> = tokenize("A  B").collect();
        assert_eq!(tokens.len(), 3);
        assert!(matches!(tokens[1], Token::WordGap));
    }

    #[test]
    fn validates_lines() {
        let cfg = Config::new("/dev/null")
            .key_line(ControlLine::Dtr)
            .ptt_line(Some(ControlLine::Dtr));
        assert!(matches!(
            validate_config(&cfg),
            Err(Error::ConflictingLines)
        ));
    }

    #[tokio::test]
    async fn serial_stream_writes_to_serial_channel() {
        let (channel, rx, _) = test_serial_channel();
        let mut stream = serial_stream_from_channel(channel);
        let write = tokio::spawn(async move { stream.write_all(b"FA;").await });

        let command = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match rx.try_recv() {
                    Ok(command) => return command,
                    Err(mpsc::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("write command channel disconnected");
                    }
                }
            }
        })
        .await
        .expect("write command is sent");
        match command.kind {
            CommandKind::WriteSerial(bytes) => assert_eq!(bytes, b"FA;"),
            _ => panic!("expected write serial command"),
        }
        command
            .ack
            .expect("write command has ack")
            .send(Ok(()))
            .expect("ack is received");

        write
            .await
            .expect("write task joins")
            .expect("stream write succeeds");
    }

    #[tokio::test]
    async fn serial_stream_reads_from_serial_channel() {
        let (channel, _rx, read_tx) = test_serial_channel();
        let mut stream = serial_stream_from_channel(channel);
        read_tx.send(b"IF;".to_vec()).expect("serial data is sent");

        let mut buf = [0_u8; 3];
        tokio::time::timeout(Duration::from_secs(1), stream.read_exact(&mut buf))
            .await
            .expect("stream read completes")
            .expect("stream read succeeds");

        assert_eq!(&buf, b"IF;");
    }
}
