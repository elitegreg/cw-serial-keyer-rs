# cw-serial-keyer

Async Rust library for keying Morse/CW on serial port modem-control lines.

## Example CLI

```sh
cargo run --example send_stdin -- --port /dev/ttyUSB0 --baud 9600 --line dtr --wpm 20 < message.txt
```

The API mirrors the basic shape of `winkeyer-rs`: open the keyer, set WPM or weighting, `send_text`, `wait_until_idle`, and `close`. Morse generation and serial-line toggling run in a dedicated blocking thread for steadier timing than a Tokio task.

```rust
use std::time::Duration;

use cw_serial_keyer::{Config, ControlLine, SerialKeyer};

# async fn demo() -> cw_serial_keyer::Result<()> {
let config = Config::new("/dev/ttyUSB0")
    .baud_rate(9600)
    .key_line(ControlLine::Dtr)
    .ptt_line(Some(ControlLine::Rts))
    .wpm(20)
    .ptt_lead_tail(Duration::from_millis(10), Duration::from_millis(10));

let mut keyer = SerialKeyer::open_with_config(config).await?;
keyer.set_weighting(50).await?;
keyer.send_text("CQ CQ DE N0CALL").await?;
keyer.wait_until_idle().await?;
keyer.close().await?;
# Ok(()) }
```

Notes:
- DTR and RTS can be driven as outputs.
- Timing follows standard Morse: dit = `1200 / wpm` ms, dash = 3 dits, character gap = 3 dits, word gap = 7 dits.
- cwdaemon was used as a reference for the serial-line model: it toggles DTR/RTS for key/PTT and uses a generated keying callback. This crate does not implement cwdaemon networking, sidetone, or escape commands.
