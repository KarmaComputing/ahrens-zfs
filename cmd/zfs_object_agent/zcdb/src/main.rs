use clap::AppSettings;
use clap::Arg;
use clap::SubCommand;
use git_version::git_version;
use zettacache::DumpStructuresOptions;
use zettacache::ZettaCacheDBCommand;

static GIT_VERSION: &str = git_version!(
    fallback = match option_env!("CARGO_ZOA_GITREV") {
        Some(value) => value,
        None => "unknown",
    }
);

#[tokio::main]
async fn main() {
    // When zcachedb is used in UNIX shell pipeline and its output is not fully
    // consumed a SIGPIPE (e.g. "broken pipe") signal is sent to us. By default,
    // we would abort and generate a core dump which is annoying. The unsafe
    // line below changes that behavior to just terminating as it is expected by
    // other UNIX utilities.
    // reference: https://github.com/rust-lang/rust/issues/46016
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }

    let matches = clap::App::new("zcachedb")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .about("ZFS ZettaCache Debugger")
        .version(GIT_VERSION)
        .arg(
            Arg::with_name("device")
                .help("ZettaCache Device")
                .required(true),
        )
        .subcommand(
            SubCommand::with_name("dump-structures")
                .about("print out on-disk structures")
                .arg(
                    Arg::with_name("nodefaults")
                        .long("nodefaults")
                        .short("n")
                        .help("skip all structures that are printed by default"),
                )
                .arg(
                    Arg::with_name("spacemaps")
                        .long("spacemaps")
                        .short("s")
                        .help("dump block allocator spacemaps"),
                )
                .arg(
                    Arg::with_name("operation-log-raw")
                        .long("operation-log-raw")
                        .help("dump operation log"),
                )
                .arg(
                    Arg::with_name("index-log-raw")
                        .long("index-log-raw")
                        .help("dump index log"),
                ),
        )
        .get_matches();

    let device = matches.value_of("device").unwrap();
    match matches.subcommand() {
        ("dump-structures", Some(subcommand_matches)) => {
            ZettaCacheDBCommand::issue_command(
                ZettaCacheDBCommand::DumpStructures(
                    DumpStructuresOptions::default()
                        .defaults(!subcommand_matches.is_present("nodefaults"))
                        .spacemaps(subcommand_matches.is_present("spacemaps"))
                        .operation_log_raw(subcommand_matches.is_present("operation-log-raw"))
                        .index_log_raw(subcommand_matches.is_present("index-log-raw")),
                ),
                device,
            )
            .await;
        }
        _ => {
            matches.usage();
        }
    };
}
