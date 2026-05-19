use std::time::Duration;

use clap::{Parser, ValueEnum};
use cw_serial_keyer::{Config, ControlLine, SerialKeyer};
use tokio::io::{self, AsyncReadExt};

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
#[command(about = "Send stdin text as CW through a serial port control line")]
struct Args {
    /// Serial device path, e.g. /dev/ttyUSB0 or /dev/ttyACM0
    #[arg(short, long)]
    port: String,

    /// Serial baud rate
    #[arg(long, default_value_t = 9600)]
    baud: u32,

    /// Serial control line to key, either dtr or rts
    #[arg(long, value_enum, default_value_t = Line::Dtr)]
    line: Line,

    /// Sending speed in words per minute
    #[arg(long, default_value_t = 20)]
    wpm: u8,

    /// Do not wait for the keyer to finish sending before closing
    #[arg(long)]
    no_wait: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let mut input = String::new();
    io::stdin().read_to_string(&mut input).await?;

    let config = Config::new(&args.port)
        .baud_rate(args.baud)
        .key_line(args.line.into());
    let mut keyer = SerialKeyer::open_with_config(config).await?;
    eprintln!("Serial keyer opened");
    keyer.set_timeout(Duration::from_secs(2));
    keyer.set_wpm(args.wpm).await?;
    keyer.send_text(input).await?;

    if !args.no_wait {
        keyer.wait_until_idle().await?;
    }

    keyer.close().await?;
    Ok(())
}
