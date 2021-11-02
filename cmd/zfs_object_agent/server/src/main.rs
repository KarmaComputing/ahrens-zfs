use clap::{Arg, SubCommand};
use git_version::git_version;
use log::*;
use zettaobject::test_connectivity;

static GIT_VERSION: &str = git_version!(
    fallback = match option_env!("CARGO_ZOA_GITREV") {
        Some(value) => value,
        None => "unknown",
    }
);

fn main() {
    let matches = clap::App::new("ZFS Object Agent")
        .about("Enables the ZFS kernel module talk to S3-protocol object storage")
        .version(GIT_VERSION)
        .arg(
            Arg::with_name("verbosity")
                .short("v")
                .multiple(true)
                .help("Sets the level of logging verbosity"),
        )
        .arg(
            Arg::with_name("socket-dir")
                .short("d")
                .long("socket-dir")
                .value_name("DIR")
                .help("Directory for unix-domain sockets")
                .takes_value(true)
                .default_value("/etc/zfs"),
        )
        .arg(
            Arg::with_name("output-file")
                .short("o")
                .long("output-file")
                .value_name("FILE")
                .help("File to log output to")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("cache-file")
                .short("c")
                .long("cache-file")
                .value_name("FILE")
                .help("File/device to use for ZettaCache")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("config-file")
                .short("t")
                .long("config-file")
                .value_name("FILE")
                .help("Configuration file to set tunables (toml/json/yaml")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("log-config")
                .short("l")
                .long("log-config")
                .value_name("FILE")
                .help("Logging configuration yaml file")
                .conflicts_with("output-file")
                .conflicts_with("verbosity")
                .takes_value(true),
        )
        .subcommand(
            SubCommand::with_name("test_connectivity")
                .about("test connectivity")
                .arg(
                    Arg::with_name("endpoint")
                        .short("e")
                        .long("endpoint")
                        .help("S3 endpoint")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("region")
                        .short("r")
                        .long("region")
                        .help("S3 region")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("bucket")
                        .short("b")
                        .long("bucket")
                        .help("S3 bucket")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("aws_access_key_id")
                        .short("i")
                        .long("aws_access_key_id")
                        .takes_value(true)
                        .requires("aws_secret_access_key")
                        .required_unless("aws_instance_profile")
                        .conflicts_with("aws_instance_profile")
                        .help("AWS access key id"),
                )
                .arg(
                    Arg::with_name("aws_secret_access_key")
                        .short("s")
                        .long("aws_secret_access_key")
                        .takes_value(true)
                        .requires("aws_access_key_id")
                        .required_unless("aws_instance_profile")
                        .conflicts_with("aws_instance_profile")
                        .help("AWS secret access key"),
                )
                .arg(
                    Arg::with_name("aws_instance_profile")
                        .long("aws_instance_profile")
                        .takes_value(false)
                        .help("Use AWS instance profile"),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        ("test_connectivity", Some(cmd_options)) => {
            let endpoint = cmd_options.value_of("endpoint").unwrap().to_string();
            let region = cmd_options.value_of("region").unwrap().to_string();
            let bucket = cmd_options.value_of("bucket").unwrap().to_string();
            let aws_access_key_id = cmd_options
                .value_of("aws_access_key_id")
                .map(str::to_string);
            let aws_secret_access_key = cmd_options
                .value_of("aws_secret_access_key")
                .map(str::to_string);
            let aws_instance_profile = cmd_options.is_present("aws_instance_profile");

            test_connectivity::test_connectivity(
                endpoint,
                region,
                bucket,
                aws_access_key_id,
                aws_secret_access_key,
                aws_instance_profile,
            );
        }
        _ => {
            let socket_dir = matches.value_of("socket-dir").unwrap();
            let cache_path = matches.value_of("cache-file");

            zettaobject::init::setup_logging(
                matches.occurrences_of("verbosity"),
                matches.value_of("output-file"),
                matches.value_of("log-config"),
            );

            if let Some(file_name) = matches.value_of("config-file") {
                util::read_tunable_config(file_name);
            }

            error!(
                "Starting ZFS Object Agent ({}).  Local timezone is {}",
                GIT_VERSION,
                chrono::Local::now().format("%Z (%:z)")
            );

            // error!() should be used when an invalid state is encountered; the related
            // operation will fail and the program may exit.  E.g. an invalid request
            // was received from the client (kernel).
            error!("logging level ERROR enabled");

            // warn!() should be used when something unexpected has happened, but it can
            // be recovered from.
            warn!("logging level WARN enabled");

            // info!() should be used for very high level operations which are expected
            // to happen infrequently (no more than once per minute in typical
            // operation).  e.g. opening/closing a pool, long-lived background tasks,
            // things that might be in `zpool history -i`.
            info!("logging level INFO enabled");

            // debug!() can be used for all but the most frequent operations.
            // e.g. not every single read/write/free operation, but perhaps for every
            // call to S3.
            debug!("logging level DEBUG enabled");

            // trace!() can be used indiscriminately.
            trace!("logging level TRACE enabled");

            zettaobject::init::start(socket_dir, cache_path);
        }
    }
}
