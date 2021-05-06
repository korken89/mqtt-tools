use log::*;
use rumqttc::{Client, Event, Incoming, MqttOptions, QoS};
use serde::Serialize;
use simple_logger::SimpleLogger;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use structopt::StructOpt;

// Reference:
// {"time": 1611137748.0325797, "qos": 0, "retain": true, "topic": "kvarntorp-test/gateway/165640a7e023861a/nodeversion", "msg_b64": "IjAuMi4xNSI="}

#[derive(Serialize, Debug)]
struct MqttMessage {
    time: f64,
    qos: u8,
    retain: bool,
    topic: String,
    msg_b64: String,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "mqtt-logger", about = "A logger of an entire MQTT stream")]
struct Opt {
    /// The verbosity of output from this program, the higher the more output one can expect
    #[structopt(short, long, env = "VERBOSITY", default_value = "2")]
    verbosity: u32,

    /// Output log file
    #[structopt(env = "OUTPUT", parse(from_os_str))]
    output: PathBuf,

    /// Server address
    #[structopt(short, long, env = "SERVER", default_value = "localhost")]
    server: String,

    /// Server port
    #[structopt(short, long, env = "PORT", default_value = "1883")]
    port: u16,
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let output = opt.output;
    let server = opt.server;
    let port = opt.port;

    match opt.verbosity {
        0 => SimpleLogger::new().with_level(log::LevelFilter::Off),
        1 => SimpleLogger::new().with_level(log::LevelFilter::Error),
        2 => SimpleLogger::new().with_level(log::LevelFilter::Info),
        3 => SimpleLogger::new().with_level(log::LevelFilter::Debug),
        _ => SimpleLogger::new().with_level(log::LevelFilter::Trace),
    }
    .init()?;

    // Ctrl+C handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        info!("Shutting down and saving log file...");
    })
    .expect("Error setting Ctrl-C handler");

    let mut log_file = BufWriter::with_capacity(
        16 * 1024 * 1024, // 16 MB cache
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output)?,
    );

    let mut mqtt_options = MqttOptions::new("mqtt-logger-sub1", &server, port);
    mqtt_options.set_keep_alive(5);
    let (mut mqtt_client, mut notifications) = Client::new(mqtt_options, 10);

    mqtt_client.subscribe("#", QoS::AtLeastOnce).unwrap();

    info!(
        "Starting logging into '{}' on address '{}:{}'...",
        output.to_str().unwrap(),
        server,
        port
    );

    for notification in notifications.iter() {
        if !running.load(Ordering::SeqCst) {
            break;
        }

        trace!("{:?}", notification);

        match notification {
            Ok(Event::Incoming(Incoming::Publish(msg))) => {
                let msg = MqttMessage {
                    time: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64(),
                    qos: msg.qos as u8,
                    retain: msg.retain,
                    topic: msg.topic,
                    msg_b64: base64::encode(&*msg.payload),
                };

                if let Ok(serialized) = serde_json::to_string(&msg) {
                    writeln!(log_file, "{}", serialized).unwrap();
                }
            }
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                debug!("Disconnected, trying to reconnect...");
                mqtt_client.subscribe("#", QoS::AtLeastOnce).unwrap();
            }
            _ => (),
        }
    }

    log_file.flush()?;

    Ok(())
}
