use std::io;

use std::path::Path;
use std::path::PathBuf;

use std::sync::Arc;

use std::ffi::OsString;

use io::BufRead;

use io::BufWriter;
use io::Write;

#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;

#[cfg(target_os = "wasi")]
use std::os::wasi::ffi::OsStringExt;

use arrow_array::RecordBatch;

use arrow_array::builder::Int64Builder;
use arrow_array::builder::StringBuilder;

use arrow_ipc::writer::StreamWriter;

use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;

pub struct DfsAncestor {
    path: PathBuf,
    serial: i64,
}

pub struct Ancestors(Vec<DfsAncestor>);

pub const ANCESTOR_CAP_DEFAULT: usize = 64;

impl Default for Ancestors {
    fn default() -> Self {
        Self::with_capacity(ANCESTOR_CAP_DEFAULT)
    }
}

impl Ancestors {
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }
}

impl Ancestors {
    pub fn get_last_id(&self) -> Option<i64> {
        self.0.last().map(|a| a.serial)
    }
}

impl Ancestors {
    pub fn push_always(&mut self, candidate: PathBuf, serial: i64) {
        self.0.push(DfsAncestor {
            path: candidate,
            serial,
        })
    }

    pub fn push_if_dir(&mut self, candidate: PathBuf, serial: i64) {
        if !candidate.is_dir() {
            return;
        }

        self.push_always(candidate, serial)
    }
}

impl Ancestors {
    pub fn resolve_parent(&mut self, child: &Path) -> Option<i64> {
        while let Some(last) = self.0.last() {
            let is_child: bool = child.starts_with(&last.path);
            let safety_check: bool = is_child && !last.path.eq(child);
            if safety_check {
                break;
            }
            self.0.pop();
        }

        self.get_last_id()
    }
}

pub fn bufrdr2path<R>(rdr: R, sep: u8) -> impl Iterator<Item = Result<PathBuf, io::Error>>
where
    R: BufRead,
{
    rdr.split(sep).map(|rslt| {
        rslt.map(|v: Vec<u8>| {
            let ostr: OsString = OsString::from_vec(v);
            PathBuf::from(ostr)
        })
    })
}

pub struct IpcStreamWriter<W> {
    wtr: StreamWriter<W>,
    sch: SchemaRef,
    batch_size: usize,
    rows_in_current_batch: usize,

    ids: Int64Builder,
    parent_ids: Int64Builder,

    paths: StringBuilder,
}

impl<W> IpcStreamWriter<W> {
    pub fn append(&mut self, id: i64, parent: Option<i64>, path: PathBuf) {
        self.ids.append_value(id);
        self.parent_ids.append_option(parent);
        let s: String = path
            .into_string()
            .unwrap_or_else(|p| p.to_string_lossy().to_string());
        self.paths.append_value(s);
        self.rows_in_current_batch += 1;
    }
}

impl<W> IpcStreamWriter<W> {
    pub fn is_empty(&self) -> bool {
        0 == self.rows_in_current_batch
    }
}

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub fn flush(&mut self) -> Result<(), io::Error> {
        if self.is_empty() {
            return Ok(());
        }

        let bat = RecordBatch::try_new(
            self.sch.clone(),
            vec![
                Arc::new(self.ids.finish()),
                Arc::new(self.parent_ids.finish()),
                Arc::new(self.paths.finish()),
            ],
        )
        .map_err(io::Error::other)?;

        self.wtr.write(&bat).map_err(io::Error::other)?;

        self.rows_in_current_batch = 0;

        Ok(())
    }
}

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub fn append_flush_if_full(
        &mut self,
        id: i64,
        parent: Option<i64>,
        path: PathBuf,
    ) -> Result<(), io::Error> {
        self.append(id, parent, path);
        if self.rows_in_current_batch == self.batch_size {
            self.flush()?;
        }
        Ok(())
    }
}

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub fn finish(&mut self) -> Result<(), io::Error> {
        self.flush()?;
        self.wtr.finish().map_err(io::Error::other)
    }
}

impl<W> IpcStreamWriter<W>
where
    W: Write,
{
    pub fn dfs_dirents2ipc<I>(&mut self, dfs_dirents: I, acap: usize) -> Result<(), io::Error>
    where
        I: Iterator<Item = Result<PathBuf, io::Error>>,
    {
        let mut stack = Ancestors::with_capacity(acap);

        for pair in dfs_dirents.enumerate() {
            let (ix, rslt) = pair;
            let pbuf: PathBuf = rslt?;
            let serial: i64 = (ix + 1) as i64;

            let parent: Option<i64> = stack.resolve_parent(&pbuf);
            self.append_flush_if_full(serial, parent, pbuf.clone())?;
            stack.push_always(pbuf, serial);
        }

        self.finish()
    }
}

pub const BAT_SIZE_DEFAULT: usize = 8192;
pub const AVG_FULLPATH_SIZE_DEFAULT: usize = 128;

pub const SEP_DEFAULT: u8 = 0x0a;

pub struct Config {
    pub batch_size: usize,

    pub avg_fullpath_size_estimate: usize,

    pub separator: u8,

    pub ancestor_cap: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            batch_size: BAT_SIZE_DEFAULT,
            avg_fullpath_size_estimate: AVG_FULLPATH_SIZE_DEFAULT,
            separator: SEP_DEFAULT,
            ancestor_cap: ANCESTOR_CAP_DEFAULT,
        }
    }
}

impl Config {
    pub fn ids_builder(&self) -> Int64Builder {
        Int64Builder::with_capacity(self.batch_size)
    }

    pub fn parents_builder(&self) -> Int64Builder {
        Int64Builder::with_capacity(self.batch_size)
    }

    pub fn paths_builder(&self) -> StringBuilder {
        StringBuilder::with_capacity(self.batch_size, self.avg_fullpath_size_estimate)
    }
}

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("parent_id", DataType::Int64, true),
        Field::new("fullpath", DataType::Utf8, false),
    ]))
}

impl Config {
    pub fn wtr2iwtr<W>(&self, wtr: W, sch: SchemaRef) -> Result<IpcStreamWriter<W>, io::Error>
    where
        W: Write,
    {
        let swtr: StreamWriter<W> = StreamWriter::try_new(wtr, &sch).map_err(io::Error::other)?;
        let ids: Int64Builder = self.ids_builder();
        let parents: Int64Builder = self.parents_builder();
        let paths: StringBuilder = self.paths_builder();
        Ok(IpcStreamWriter {
            wtr: swtr,
            sch,
            batch_size: self.batch_size,
            rows_in_current_batch: 0,
            ids,
            parent_ids: parents,
            paths,
        })
    }
}

impl Config {
    pub fn rdr2paths2ipc2wtr<R, W>(&self, rdr: R, mut wtr: W) -> Result<(), io::Error>
    where
        R: BufRead,
        W: Write,
    {
        let ipat = bufrdr2path(rdr, self.separator); // dirents
        let sch: SchemaRef = schema();
        let mut isw: IpcStreamWriter<_> = self.wtr2iwtr(&mut wtr, sch)?;
        isw.dfs_dirents2ipc(ipat, self.ancestor_cap)?;
        wtr.flush()
    }
}

impl Config {
    pub fn stdin2paths2ipc2stdout(&self) -> Result<(), io::Error> {
        let o = io::stdout();
        let mut ol = o.lock();
        self.rdr2paths2ipc2wtr(io::stdin().lock(), BufWriter::new(&mut ol))?;
        ol.flush()
    }
}
