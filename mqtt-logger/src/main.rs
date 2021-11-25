use anyhow::anyhow;
use chrono::{DateTime, SecondsFormat, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use log::*;
use rumqttc::{
    Client, ClientConfig, ConnectionError, Event, Incoming, MqttOptions, Outgoing, QoS,
    TlsConfiguration, Transport,
};
use serde::Serialize;
use simple_logger::SimpleLogger;
use std::borrow::Cow;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
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
    #[structopt(short, long, env = "VERBOSITY", default_value = "0")]
    verbosity: u32,

    /// Output log file
    #[structopt(env = "OUTPUT", parse(from_os_str))]
    output: PathBuf,

    /// ZSTD compression level
    #[structopt(short, long, env = "COMPRESSION_LEVEL", default_value = "9")]
    compression_level: i32,

    /// Server address
    #[structopt(short, long, env = "SERVER", default_value = "localhost")]
    server: String,

    /// Server port
    #[structopt(short, long, env = "PORT")]
    port: Option<u16>,

    /// TLS enable
    #[structopt(long, env = "TLS")]
    tls: bool,

    /// Path to custom CA file
    #[structopt(long, env = "CUSTOM_CA")]
    custom_ca: Option<PathBuf>,

    /// An optional duration for how long to log, e.g. 100s, 12h, 1year, etc.
    #[structopt(long, required_if("forever", "true"), env = "DURATION")]
    duration: Option<String>,

    /// If this is set it will log and save a new file with the period set by duration.
    #[structopt(long, env = "FOREVER")]
    forever: bool,

    /// Topic to subscribe to. Supports multiple.
    #[structopt(long, env = "TOPIC", default_value = "#")]
    topic: Vec<String>,
}

fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let server = opt.server;
    let port = opt.port.unwrap_or(if opt.tls || opt.custom_ca.is_some() {
        8883
    } else {
        1883
    });
    let compression_level = opt.compression_level;
    let duration = if let Some(s) = &opt.duration {
        Some(parse_duration::parse(&s).expect(&format!(
            "Unable to parse the --duration argument: '{}'",
            &s
        )))
    } else {
        None
    };
    let forever = opt.forever;
    let mut output = if !forever {
        opt.output.clone()
    } else {
        if duration.unwrap() < Duration::from_secs(1) {
            return Err(anyhow!(
                "The duration needs to be more than 1 second when using --forever"
            ));
        }

        let mut output = opt.output.clone();

        let utc: DateTime<Utc> = Utc::now();
        let filename = output
            .file_stem()
            .expect("Empty filename?")
            .to_str()
            .expect("Non-unicode path?");
        let now_time = format!(
            "{}-{}",
            filename,
            utc.to_rfc3339_opts(SecondsFormat::Secs, false)
        );

        output.set_file_name(now_time);

        println!("output: {:?}", output);

        output
    };

    output.set_extension("json.zst");

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

    let log_file = BufWriter::with_capacity(
        128 * 1024, // 128 kB cache
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output)?,
    );
    let mut log_file = zstd::Encoder::new(log_file, compression_level)?.auto_finish();

    // -------------------------- MQTT Start ---------------------------

    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::new(0, 1))
        .subsec_nanos();
    let mut mqtt_options = MqttOptions::new(&format!("mqtt-logger-sub{}", nanos), &server, port);

    // Check for custom CA file
    let custom_ca = if let Some(custom_ca_path) = &opt.custom_ca {
        use std::io::prelude::*;

        let ca_str = custom_ca_path.to_str();

        if !(ca_str == Some("null") || ca_str == Some("None") || ca_str == Some("none")) {
            let mut vec = Vec::new();

            fs::File::open(custom_ca_path)
                .expect("Could not open CA file")
                .read_to_end(&mut vec)
                .expect("Could not read specified CA file");

            Some(vec)
        } else {
            None
        }
    } else {
        None
    };

    // Encrypted MQTT?
    if opt.tls || custom_ca.is_some() {
        let transport = if let Some(custom_ca) = &custom_ca {
            Transport::Tls(TlsConfiguration::Simple {
                ca: custom_ca.clone(),
                alpn: None,
                client_auth: None,
            })
        } else {
            let mut client_config = ClientConfig::new();
            // Use rustls-native-certs to load root certificates from the operating system.
            client_config.root_store = rustls_native_certs::load_native_certs()
                .expect("Failed to load platform certificates.");

            Transport::tls_with_config(client_config.into())
        };

        mqtt_options.set_transport(transport);
    }

    mqtt_options.set_keep_alive(5);
    let (mut mqtt_client, mut notifications) = Client::new(mqtt_options, 10);

    if opt.topic.is_empty() {
        return Err(anyhow!("No topics supplied"));
    }

    for topic in &opt.topic {
        mqtt_client.subscribe(topic, QoS::AtLeastOnce)?;
    }

    // -------------------------- MQTT END ---------------------------
    println!(
        "Starting logging with ZSTD compression (level {}) into '{}' on address '{}:{}'",
        compression_level,
        output.to_str().unwrap(),
        server,
        port
    );

    for topic in &opt.topic {
        println!("    - Subscribing to topic '{}'", topic);
    }

    if opt.tls || custom_ca.is_some() {
        let certs: String = if custom_ca.is_some() {
            format!(
                "custom CA loaded from '{}'",
                opt.custom_ca.unwrap().to_str().unwrap()
            )
        } else {
            "using native certs".into()
        };

        println!("    - using TLS ({})", certs,);
    }

    if let Some(dur) = &duration {
        if forever {
            println!(
                "    - Running forever, saving logfiles every {:?} ({})",
                dur,
                opt.duration.unwrap()
            );
        } else {
            println!("    - Stopping after {:?} ({})", dur, opt.duration.unwrap());
        }
    }

    let pb = ProgressBar::new_spinner();
    pb.enable_steady_tick(80);
    pb.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&[
                "[    ]", "[=   ]", "[==  ]", "[=== ]", "[ ===]", "[  ==]", "[   =]", "[    ]",
                "[   =]", "[  ==]", "[ ===]", "[====]", "[=== ]", "[==  ]", "[=   ]",
            ])
            .template("{spinner} {msg}"),
    );
    pb.set_message("Logging... No messages recorded yet.");

    let mut count: u64 = 0;
    let mut bytes_written = 0.;
    let mut connected = true;
    let time_start = SystemTime::now();
    let mut duration_check = time_start;

    for notification in notifications.iter() {
        if !running.load(Ordering::SeqCst) {
            pb.finish();
            break;
        }

        if let Some(dur) = duration {
            if SystemTime::now().duration_since(duration_check)? > dur {
                if forever {
                    duration_check = SystemTime::now();
                    let mut output = opt.output.clone();

                    let utc: DateTime<Utc> = Utc::now();
                    let filename = output
                        .file_stem()
                        .expect("Empty filename?")
                        .to_str()
                        .expect("Non-unicode path?");
                    let now_time = format!(
                        "{}-{}",
                        filename,
                        utc.to_rfc3339_opts(SecondsFormat::Secs, false)
                    );

                    output.set_file_name(now_time);
                    output.set_extension("json.zst");

                    let lf = BufWriter::with_capacity(
                        128 * 1024, // 128 kB cache
                        fs::OpenOptions::new()
                            .write(true)
                            .create_new(true)
                            .open(&output)?,
                    );
                    log_file = zstd::Encoder::new(lf, compression_level)?.auto_finish();
                } else {
                    running.store(false, Ordering::SeqCst);
                }
            }
        }

        match notification {
            Ok(Event::Incoming(Incoming::Publish(msg))) => {
                let msg = MqttMessage {
                    time: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)?
                        .as_secs_f64(),
                    qos: msg.qos as u8,
                    retain: msg.retain,
                    topic: msg.topic,
                    msg_b64: base64::encode(&*msg.payload),
                };

                if let Ok(serialized) = serde_json::to_string(&msg) {
                    count += 1;
                    bytes_written += serialized.len() as f64 + 2.; // 2 = newline

                    pb.set_message(Cow::Owned(format!(
                        "Logging... {} messages recorded, uncompressed data size: {:.2} MB.",
                        count,
                        bytes_written / 1024. / 1024.,
                    )));
                    writeln!(log_file, "{}", serialized)?
                }
            }
            Ok(Event::Incoming(Incoming::Disconnect)) => {
                debug!("Disconnected, trying to reconnect...");
                connected = false;
            }
            Ok(Event::Outgoing(Outgoing::PingReq)) => {
                if !connected {
                    debug!("Trying to resubscribe...");

                    for topic in &opt.topic {
                        mqtt_client.subscribe(topic, QoS::AtLeastOnce)?;
                    }

                    connected = true;
                }
            }
            Ok(val) => trace!("Unhandled Ok(...) notification: {:?}", val),
            Err(val) => match val {
                ConnectionError::MqttState(e) => {
                    debug!("MQTT error, will try to reconnect when possible: {:?}", e);
                    connected = false;
                }
                ConnectionError::Network(e) => {
                    debug!(
                        "Network error, will try to reconnect when possible: {:?}",
                        e
                    );
                    connected = false;
                }
                _ => {
                    trace!("Unhandled Err(...) notification: {:?}", val);
                    connected = false;
                }
            },
        }
    }

    log_file.flush()?;

    Ok(())
}
