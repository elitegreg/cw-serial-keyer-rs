# cw-serial-keyer

Async Rust library for keying Morse/CW on serial port modem-control lines.

## Example CLI

```sh
cargo run --example send_stdin -- --port /dev/ttyUSB0 --baud 9600 --line dtr --wpm 20 < message.txt
```

TCP serial proxy support is optional at runtime. The library keeps serial
reads/writes inside the keyer worker and exposes them through `SerialChannel`;
`start_tcp_server` builds a Tokio async TCP listener on top of that channel
while the keyer still uses DTR/RTS for Morse keying:

```sh
cargo run --example tcp_proxy_beacon -- \
  --port /dev/ttyUSB0 --baud 9600 --listen 127.0.0.1:7373 --every 30
```

The `tcp_proxy_beacon` example sends `hello` every 30 seconds by default and
proxies one TCP client at a time to the same serial port.

Applications can also use the raw serial channel directly:

```rust
# async fn demo_serial(mut keyer: cw_serial_keyer::SerialKeyer) -> cw_serial_keyer::Result<()> {
let mut serial = keyer.serial_channel();
serial.write(b"radio command\r").await?;
let bytes = serial.read().await?;
# let _ = bytes;
# Ok(()) }
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
