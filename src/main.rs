use std::ffi::OsStr;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{ensure, Context, Error};
use flate2::write::GzEncoder;
use flate2::Compression;
use log::{error, info, warn, LevelFilter};
use maildir::{MailEntry, Maildir};
use mailparse::MailHeaderMap;
use structopt::StructOpt;

/// Tool to archive too old emails.
///
/// Either deletes them or puts them to a maildbox file (optionally gzipped one).
#[derive(Debug, StructOpt)]
struct Opts {
    /// The maildir to process and search for old messages.
    #[structopt(short = "d", long = "dir", parse(from_os_str))]
    maildir: PathBuf,

    /// Where to put the old messages.
    #[structopt(short = "a", long = "archive", parse(from_os_str))]
    archive: Option<PathBuf>,

    /// Remove messages instead of archiving.
    #[structopt(short = "r", long = "remove")]
    remove: bool,

    /// Don't do a dry run only, actually run the actions.
    #[structopt(short = "c", long = "confirm")]
    confirm: bool,

    /// Process "new" old emails too.
    #[structopt(short = "n", long = "new")]
    new: bool,

    /// Age in days.
    #[structopt(short = "A", long = "age", default_value = "30")]
    age: usize,
}

impl Opts {
    fn check(&self) -> Result<(), Error> {
        ensure!(
            self.maildir.is_dir(),
            "Maildir {} does not exist",
            self.maildir.display()
        );
        ensure!(
            self.archive.is_some() ^ self.remove,
            "You can either archive or remove, not both"
        );

        Ok(())
    }

    fn destination(&self) -> Result<Box<dyn Write + Send + Sync>, Error> {
        if self.remove {
            Ok(Box::new(io::sink()))
        } else {
            let filename = self
                .archive
                .as_ref()
                .expect("Already checked we have the file set");
            let out = OpenOptions::new()
                .read(false)
                .write(true)
                .create(true)
                .truncate(false)
                .append(true)
                .open(filename)
                .with_context(|| format!("Failed to write {}", filename.display()))?;
            let out = BufWriter::new(out);

            if filename.extension() == Some(OsStr::new("gz")) {
                let out = GzEncoder::new(out, Compression::best());
                Ok(Box::new(out))
            } else {
                Ok(Box::new(out))
            }
        }
    }
}

struct Criteria {
    before: i64,
    must_seen: bool,
}

impl Criteria {
    fn new(age: usize, must_seen: bool) -> Self {
        let now = SystemTime::now();
        let before = now - Duration::from_secs(3600 * 24 * (age as u64));
        let before = before
            .duration_since(UNIX_EPOCH)
            .expect("Time before epoch");
        Self {
            before: before.as_secs() as i64,
            must_seen,
        }
    }

    fn should_archive(&self, mail: &MailInfo) -> bool {
        let old = mail.date_resolved <= self.before;
        old && (!self.must_seen || mail.seen) && !mail.flagged
    }
}

struct MailInfo {
    subject: String,
    date: String,
    date_resolved: i64,
    id: String,
    path: PathBuf,
    seen: bool,
    flagged: bool,
}

impl MailInfo {
    fn new(mail: &mut MailEntry) -> Result<Self, Error> {
        let seen = mail.is_seen();
        let flagged = mail.is_flagged();
        let date_resolved = mail.date().context("Broken Date header")?;
        let headers = mail.parsed().context("Can't parse mail")?;
        let headers = headers.get_headers();
        let date = headers.get_first_value("Date").unwrap_or_default();
        let subject = headers.get_first_value("Subject").unwrap_or_default();
        Ok(Self {
            subject,
            date,
            date_resolved,
            id: mail.id().to_owned(),
            path: mail.path().to_owned(),
            seen,
            flagged,
        })
    }

    fn archive(&self, dest: &mut dyn Write) -> Result<(), Error> {
        let infile = File::open(&self.path)
            .with_context(|| format!("Failed to open {}", self.path.display()))?;
        let filtered = Command::new("formail")
            .args(&["-I", "Status: RO"])
            .stdin(infile)
            .output()
            .with_context(|| format!("Couldn't formail {}", self.path.display()))?;
        ensure!(
            filtered.status.success(),
            "Formail on {} failed: {}",
            self.path.display(),
            filtered.status.success()
        );

        dest.write_all(&filtered.stdout)
            .context("Failed to output email")?;

        Ok(())
    }
}

impl Display for MailInfo {
    fn fmt(&self, fmt: &mut Formatter) -> FmtResult {
        write!(fmt, "{}/{}/{}", self.id, self.date, self.subject)
    }
}

fn main() -> Result<(), Error> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    let opts = Opts::from_args();
    opts.check()?;

    let mut dest = opts
        .destination()
        .context("Failed to open the destination")?;

    let dir = Maildir::from(opts.maildir);
    let mails = dir.list_cur();
    let mails = if opts.new {
        Box::new(mails.chain(dir.list_new())) as Box<dyn Iterator<Item = _>>
    } else {
        Box::new(mails)
    };

    let criteria = Criteria::new(opts.age, !opts.new);
    let mut archived = 0usize;
    let mut kept = 0usize;
    let mut parse_err = 0usize;
    let mut move_err = 0usize;

    for mail in mails {
        let mail = mail.map_err(Error::from).and_then(|mut m| {
            MailInfo::new(&mut m).with_context(|| format!("Failed to parse email {}", m.id()))
        });

        match mail {
            Ok(mail) => {
                if criteria.should_archive(&mail) {
                    info!("Archive {}", mail);
                    if opts.confirm {
                        let deleted = mail
                            .archive(&mut dest)
                            .with_context(|| format!("Failed to move mail {}", mail))
                            .and_then(|()| {
                                dir.delete(&mail.id)
                                    .with_context(|| format!("Failed to delete mail {}", mail))
                            });
                        match deleted {
                            Ok(()) => archived += 1,
                            Err(e) => {
                                error!("{:?}", e);
                                move_err += 1;
                            }
                        }
                    }
                } else {
                    kept += 1;
                }
            }
            Err(e) => {
                error!("{:?}", e);
                parse_err += 1;
            }
        }
    }

    info!("Archived: {}", archived);
    info!("Kept: {}", kept);
    if parse_err > 0 {
        warn!("Parse errors: {}", parse_err);
    }
    if move_err > 0 {
        warn!("Move errors: {}", move_err);
    }

    Ok(())
}
