use std::net::SocketAddr;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use cw_serial_keyer::{Config, ControlLine, SerialKeyer};
use tokio::signal;
use tokio::time::{interval_at, Instant, MissedTickBehavior};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Line {
    Dtr,
    Rts,
}

impl From<Line> for ControlLine {
    fn from(line: Line) -> Self {
        match line {
            Line::Dtr => ControlLine::Dtr,
            Line::Rts => ControlLine::Rts,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Proxy a serial port over TCP while sending a periodic CW beacon")]
struct Args {
    /// Serial device path, e.g. /dev/ttyUSB0 or /dev/ttyACM0
    #[arg(short, long)]
    port: String,

    /// Serial baud rate
    #[arg(long, default_value_t = 9600)]
    baud: u32,

    /// TCP listen address for the serial byte proxy
    #[arg(long, default_value = "127.0.0.1:7373")]
    listen: SocketAddr,

    /// Serial control line to key, either dtr or rts
    #[arg(long, value_enum, default_value_t = Line::Dtr)]
    line: Line,

    /// Sending speed in words per minute
    #[arg(long, default_value_t = 20)]
    wpm: u8,

    /// Beacon text to send
    #[arg(long, default_value = "hello")]
    text: String,

    /// Seconds between beacons
    #[arg(long, default_value_t = 30)]
    every: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let config = Config::new(&args.port)
        .baud_rate(args.baud)
        .key_line(args.line.into())
        .wpm(args.wpm);
    let mut keyer = SerialKeyer::open_with_config(config).await?;
    keyer.set_timeout(Duration::from_secs(2));

    let mut server = keyer.start_tcp_server(args.listen).await?;
    eprintln!("Serial TCP proxy listening on {}", server.local_addr());
    eprintln!(
        "Sending {:?} every {} seconds; press Ctrl-C to stop",
        args.text, args.every
    );

    let period = Duration::from_secs(args.every);
    let mut ticker = interval_at(Instant::now() + period, period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => keyer.send_text(&args.text).await?,
            _ = signal::ctrl_c() => break,
        }
    }

    server.close().await;
    keyer.close().await?;
    Ok(())
}
