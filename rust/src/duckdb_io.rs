//! An `ObjectStore` whose reads go through the engine's file system, so a table
//! opened with it reads its files through DuckDB's external file cache.
//!
//! The engine supplies one read callback. Each call is stateless: it opens the
//! file, reports its size, modification time and version tag through the
//! [`HudiReadSink`], is told which bytes to copy, and copies them into a buffer
//! the sink owns. Listing, writes, deletes and copies go to a fallback store
//! built from the table URI and storage options the way hudi-rs builds its own.
#![allow(clippy::missing_safety_doc)]

use std::ffi::{c_char, c_void, CString};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use hudi::object_store::path::Path;
use hudi::object_store::{
    self, Attributes, CopyOptions, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreScheme, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use url::{Position, Url};

/// The read succeeded.
pub const HUDI_IO_OK: i32 = 0;
/// The file does not exist.
pub const HUDI_IO_NOT_FOUND: i32 = 1;
/// Any other failure; the message is in the sink.
pub const HUDI_IO_ERROR: i32 = 2;

/// Read `uri` (NUL-terminated) into `sink`. Called from tokio blocking threads,
/// concurrently, so it must be thread-safe.
pub type HudiReadFn =
    unsafe extern "C" fn(ctx: *mut c_void, uri: *const c_char, sink: *mut HudiReadSink) -> i32;

/// Release `ctx`. Called once, when the last store using it is dropped, from
/// whichever thread drops it.
pub type HudiReleaseFn = unsafe extern "C" fn(ctx: *mut c_void);

/// The engine's file I/O: an opaque context and the callbacks that use it.
#[repr(C)]
pub struct HudiFileIo {
    pub ctx: *mut c_void,
    pub read: Option<HudiReadFn>,
    pub release: Option<HudiReleaseFn>,
}

/// Owns the engine's context and releases it on drop.
pub struct FileIo {
    ctx: *mut c_void,
    read: HudiReadFn,
    release: Option<HudiReleaseFn>,
}

// The callbacks are thread-safe by contract, and the context is only passed
// back to them.
unsafe impl Send for FileIo {}
unsafe impl Sync for FileIo {}

impl FileIo {
    /// Take ownership of `io.ctx`. It is released on every path, including
    /// when `io` has no read callback.
    pub unsafe fn take(io: &HudiFileIo) -> Result<Self, String> {
        let owned = FileIo {
            ctx: io.ctx,
            read: io.read.unwrap_or(noop_read),
            release: io.release,
        };
        if io.read.is_none() {
            return Err("hudi: the file I/O has no read callback".into());
        }
        Ok(owned)
    }

    fn read(&self, uri: &CString, request: Request) -> Result<Fetched, IoFailure> {
        let mut sink = HudiReadSink::new(request);
        let status = unsafe { (self.read)(self.ctx, uri.as_ptr(), &mut sink) };
        sink.finish(status)
    }
}

unsafe extern "C" fn noop_read(_: *mut c_void, _: *const c_char, _: *mut HudiReadSink) -> i32 {
    HUDI_IO_ERROR
}

impl Drop for FileIo {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self.ctx) };
        }
    }
}

/// What one read asks for.
#[derive(Clone, Debug)]
enum Request {
    Head,
    Bytes(Option<GetRange>),
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// What the engine reported for one read.
#[derive(Debug)]
struct Fetched {
    size: u64,
    last_modified_us: i64,
    version: Option<String>,
    range: Range<u64>,
    data: Vec<u8>,
}

#[derive(Debug)]
enum IoFailure {
    NotFound(String),
    InvalidRange(BoxError),
    Other(String),
}

/// Collects one read's result. The engine calls the `hudi_read_sink_*`
/// functions on it during its read callback, on the same thread.
pub struct HudiReadSink {
    request: Request,
    info: Option<(u64, i64)>,
    version: Option<String>,
    range: Range<u64>,
    data: Vec<u8>,
    invalid_range: Option<BoxError>,
    error: Option<String>,
}

impl HudiReadSink {
    fn new(request: Request) -> Self {
        Self {
            request,
            info: None,
            version: None,
            range: 0..0,
            data: Vec::new(),
            invalid_range: None,
            error: None,
        }
    }

    /// Record the file's size and modification time, and choose the bytes to
    /// read: `None` when none are wanted.
    fn set_info(&mut self, size: u64, last_modified_us: i64) -> Option<Range<u64>> {
        self.info = Some((size, last_modified_us));
        let range = match &self.request {
            Request::Head => return None,
            Request::Bytes(None) => 0..size,
            Request::Bytes(Some(range)) => match range.as_range(size) {
                Ok(range) => range,
                Err(e) => {
                    self.invalid_range = Some(Box::new(e));
                    return None;
                }
            },
        };
        // Left uninitialized: the engine fills all of it before reporting success.
        self.data = Vec::with_capacity((range.end - range.start) as usize);
        self.range = range.clone();
        (!range.is_empty()).then_some(range)
    }

    fn finish(self, status: i32) -> Result<Fetched, IoFailure> {
        let message = || {
            self.error
                .clone()
                .unwrap_or_else(|| format!("the engine read failed with status {status}"))
        };
        match status {
            HUDI_IO_OK => {}
            HUDI_IO_NOT_FOUND => return Err(IoFailure::NotFound(message())),
            _ => return Err(IoFailure::Other(message())),
        }
        if let Some(e) = self.invalid_range {
            return Err(IoFailure::InvalidRange(e));
        }
        let (size, last_modified_us) = self
            .info
            .ok_or_else(|| IoFailure::Other("the engine read reported no file size".into()))?;
        let mut data = self.data;
        // SAFETY: the capacity was reserved for exactly this range, and a read
        // that reports success has written every byte of it.
        unsafe { data.set_len((self.range.end - self.range.start) as usize) };
        Ok(Fetched {
            size,
            last_modified_us,
            version: self.version,
            range: self.range,
            data,
        })
    }
}

/// Report the file's size and modification time (microseconds since the
/// epoch). Returns 1 and fills `offset`, `len` and `buf` when the caller must
/// copy `len` bytes starting at `offset` of the file into `buf`; returns 0 when
/// no bytes are wanted. `buf` is uninitialized, and the read may report
/// `HUDI_IO_OK` only after writing all `len` bytes.
#[no_mangle]
pub unsafe extern "C" fn hudi_read_sink_set_info(
    sink: *mut HudiReadSink,
    size: u64,
    last_modified_us: i64,
    offset: *mut u64,
    len: *mut usize,
    buf: *mut *mut u8,
) -> i32 {
    let Some(sink) = sink.as_mut() else {
        return 0;
    };
    match sink.set_info(size, last_modified_us) {
        Some(range) if !offset.is_null() && !len.is_null() && !buf.is_null() => {
            *offset = range.start;
            *len = (range.end - range.start) as usize;
            *buf = sink.data.as_mut_ptr();
            1
        }
        _ => 0,
    }
}

/// Record the file's version tag (an ETag for S3).
#[no_mangle]
pub unsafe extern "C" fn hudi_read_sink_set_version(
    sink: *mut HudiReadSink,
    tag: *const c_char,
    len: usize,
) {
    if let (Some(sink), false) = (sink.as_mut(), tag.is_null()) {
        let tag = std::slice::from_raw_parts(tag as *const u8, len);
        if !tag.is_empty() {
            sink.version = Some(String::from_utf8_lossy(tag).into_owned());
        }
    }
}

/// Record the message of a failed read.
#[no_mangle]
pub unsafe extern "C" fn hudi_read_sink_set_error(
    sink: *mut HudiReadSink,
    message: *const c_char,
    len: usize,
) {
    if let (Some(sink), false) = (sink.as_mut(), message.is_null()) {
        let message = std::slice::from_raw_parts(message as *const u8, len);
        sink.error = Some(String::from_utf8_lossy(message).into_owned());
    }
}

const STORE_NAME: &str = "DuckDB";

/// The store root of `url`: the URI prefix that the object paths of the store
/// `object_store` builds for `url` are relative to. It is the scheme and
/// authority, plus the bucket or container when `object_store` takes it from the
/// path.
pub(crate) fn store_root(url: &Url) -> Result<String, String> {
    let (_, store_path) = ObjectStoreScheme::parse(url)
        .map_err(|e| format!("hudi: unsupported table URI {url}: {e}"))?;
    let full_path = Path::from_url_path(url.path())
        .map_err(|e| format!("hudi: invalid table URI path {url}: {e}"))?;
    let authority = &url[..Position::BeforePath];
    if store_path == full_path {
        return Ok(authority.to_string());
    }
    let bucket = full_path
        .parts()
        .next()
        .ok_or_else(|| format!("hudi: table URI {url} has no bucket in its path"))?;
    Ok(format!("{authority}/{}", bucket.as_ref()))
}

/// Reads through the engine's file I/O; everything else through `fallback`.
pub struct DuckDbObjectStore {
    root: String,
    io: Arc<FileIo>,
    fallback: Arc<dyn ObjectStore>,
}

impl DuckDbObjectStore {
    pub(crate) fn new(root: String, io: FileIo, fallback: Arc<dyn ObjectStore>) -> Self {
        // A local object path is the absolute path without its leading `/`, so
        // a local table maps to plain paths rather than `file://` URIs.
        let root = if root == "file://" {
            String::new()
        } else {
            root
        };
        Self {
            root,
            io: Arc::new(io),
            fallback,
        }
    }

    /// The full URI of `location` under the store root.
    fn uri_of(&self, location: &Path) -> object_store::Result<CString> {
        CString::new(format!("{}/{location}", self.root)).map_err(|e| {
            object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(e),
            }
        })
    }

    async fn fetch(&self, location: &Path, request: Request) -> object_store::Result<Fetched> {
        let uri = self.uri_of(location)?;
        let io = Arc::clone(&self.io);
        // The engine's read blocks, so it runs on the blocking pool rather than
        // on a runtime worker.
        let outcome = tokio::task::spawn_blocking(move || io.read(&uri, request))
            .await
            .map_err(|e| object_store::Error::Generic {
                store: STORE_NAME,
                source: Box::new(e),
            })?;
        outcome.map_err(|failure| to_object_store_error(location, failure))
    }

    fn meta(location: &Path, fetched: &Fetched) -> ObjectMeta {
        let last_modified =
            UNIX_EPOCH + Duration::from_micros(fetched.last_modified_us.max(0) as u64);
        ObjectMeta {
            location: location.clone(),
            last_modified: last_modified.into(),
            size: fetched.size,
            e_tag: fetched.version.clone(),
            version: None,
        }
    }
}

fn to_object_store_error(location: &Path, failure: IoFailure) -> object_store::Error {
    match failure {
        IoFailure::NotFound(message) => object_store::Error::NotFound {
            path: location.to_string(),
            source: message.into(),
        },
        IoFailure::InvalidRange(source) => object_store::Error::Generic {
            store: STORE_NAME,
            source,
        },
        IoFailure::Other(message) => object_store::Error::Generic {
            store: STORE_NAME,
            source: format!("{location}: {message}").into(),
        },
    }
}

/// Whether `options` carries a precondition the engine's read cannot check.
fn has_preconditions(options: &GetOptions) -> bool {
    options.if_match.is_some()
        || options.if_none_match.is_some()
        || options.if_modified_since.is_some()
        || options.if_unmodified_since.is_some()
        || options.version.is_some()
}

impl fmt::Debug for DuckDbObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDbObjectStore")
            .field("root", &self.root)
            .field("fallback", &self.fallback)
            .finish()
    }
}

impl fmt::Display for DuckDbObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DuckDbObjectStore({})", self.root)
    }
}

#[async_trait::async_trait]
impl ObjectStore for DuckDbObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.fallback.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.fallback.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if has_preconditions(&options) {
            return self.fallback.get_opts(location, options).await;
        }
        let request = if options.head {
            Request::Head
        } else {
            Request::Bytes(options.range.clone())
        };
        let fetched = self.fetch(location, request).await?;
        let meta = Self::meta(location, &fetched);
        let range = fetched.range.clone();
        let bytes = Bytes::from(fetched.data);
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        // Coalesced the way object_store's default does for hudi-rs's own
        // stores: one engine read per merged range, the requested ranges sliced
        // out of it without copying.
        object_store::coalesce_ranges(
            ranges,
            |range| async move {
                if range.is_empty() {
                    return Ok(Bytes::new());
                }
                let fetched = self
                    .fetch(location, Request::Bytes(Some(GetRange::Bounded(range))))
                    .await?;
                Ok(Bytes::from(fetched.data))
            },
            object_store::OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.fallback.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.fallback.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.fallback.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.fallback.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.fallback.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use hudi::object_store::memory::InMemory;
    use hudi::object_store::ObjectStoreExt;
    use std::collections::HashMap;
    use std::ffi::CStr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    /// Serves reads from a map of URI to bytes, or from the local file system
    /// for `file://` URIs it does not hold, and records what it was asked.
    #[derive(Default)]
    pub(crate) struct MockIo {
        pub files: HashMap<String, Vec<u8>>,
        pub errors: HashMap<String, String>,
        pub uris: Mutex<Vec<String>>,
        pub released: Arc<AtomicUsize>,
    }

    unsafe extern "C" fn mock_read(
        ctx: *mut c_void,
        uri: *const c_char,
        sink: *mut HudiReadSink,
    ) -> i32 {
        let io = &*(ctx as *const MockIo);
        let uri = CStr::from_ptr(uri).to_str().unwrap().to_string();
        io.uris.lock().unwrap().push(uri.clone());
        if let Some(message) = io.errors.get(&uri) {
            hudi_read_sink_set_error(sink, message.as_ptr() as *const c_char, message.len());
            return HUDI_IO_ERROR;
        }
        let local;
        let data = match io.files.get(&uri) {
            Some(data) => data,
            None => match uri.starts_with('/').then(|| std::fs::read(&uri)) {
                Some(Ok(bytes)) => {
                    local = bytes;
                    &local
                }
                _ => {
                    let message = format!("no such file {uri}");
                    hudi_read_sink_set_error(
                        sink,
                        message.as_ptr() as *const c_char,
                        message.len(),
                    );
                    return HUDI_IO_NOT_FOUND;
                }
            },
        };
        let tag = "tag-1";
        hudi_read_sink_set_version(sink, tag.as_ptr() as *const c_char, tag.len());
        let (mut offset, mut len, mut buf) = (0u64, 0usize, std::ptr::null_mut());
        if hudi_read_sink_set_info(
            sink,
            data.len() as u64,
            1_000_000,
            &mut offset,
            &mut len,
            &mut buf,
        ) == 1
        {
            let start = offset as usize;
            std::ptr::copy_nonoverlapping(data[start..start + len].as_ptr(), buf, len);
        }
        HUDI_IO_OK
    }

    unsafe extern "C" fn mock_release(ctx: *mut c_void) {
        let io = Box::from_raw(ctx as *mut MockIo);
        io.released.fetch_add(1, Ordering::SeqCst);
    }

    /// A `HudiFileIo` over `mock`, which it owns until released. The returned
    /// pointer stays valid until then.
    pub(crate) fn mock_file_io(mock: MockIo) -> (HudiFileIo, *const MockIo) {
        let ptr = Box::into_raw(Box::new(mock));
        (
            HudiFileIo {
                ctx: ptr as *mut c_void,
                read: Some(mock_read),
                release: Some(mock_release),
            },
            ptr,
        )
    }

    fn store_over(mock: MockIo, root: &str) -> (DuckDbObjectStore, *const MockIo) {
        let (io, ptr) = mock_file_io(mock);
        let io = unsafe { FileIo::take(&io) }.unwrap();
        (
            DuckDbObjectStore::new(root.to_string(), io, Arc::new(InMemory::new())),
            ptr,
        )
    }

    fn root_of(uri: &str) -> String {
        store_root(&Url::parse(uri).unwrap()).unwrap()
    }

    #[test]
    fn store_root_matches_the_object_paths_of_each_scheme() {
        assert_eq!(root_of("s3://bucket/a/b"), "s3://bucket");
        assert_eq!(root_of("s3a://bucket/a/b"), "s3a://bucket");
        assert_eq!(root_of("gs://bucket/a"), "gs://bucket");
        assert_eq!(root_of("az://container/a/b"), "az://container");
        assert_eq!(root_of("file:///tmp/t"), "file://");
        assert_eq!(
            root_of("https://acct.blob.core.windows.net/container/a/b"),
            "https://acct.blob.core.windows.net/container"
        );
        assert!(store_root(&Url::parse("unknown://host/a").unwrap()).is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reads_map_object_paths_to_uris_under_the_root() {
        let mut mock = MockIo::default();
        mock.files.insert(
            "s3://bucket/warehouse/t1/.hoodie/hoodie.properties".into(),
            b"0123456789".to_vec(),
        );
        let (store, mock) = store_over(mock, &root_of("s3://bucket/warehouse/t1"));
        let path = Path::from("warehouse/t1/.hoodie/hoodie.properties");

        let all = store.get(&path).await.unwrap();
        assert_eq!(all.meta.size, 10);
        assert_eq!(all.meta.e_tag.as_deref(), Some("tag-1"));
        assert_eq!(all.range, 0..10);
        assert_eq!(all.bytes().await.unwrap().as_ref(), b"0123456789");

        assert_eq!(store.get_range(&path, 2..5).await.unwrap().as_ref(), b"234");
        let suffix = store
            .get_opts(
                &path,
                GetOptions::new().with_range(Some(GetRange::Suffix(3))),
            )
            .await
            .unwrap();
        assert_eq!(suffix.range, 7..10);
        assert_eq!(suffix.bytes().await.unwrap().as_ref(), b"789");
        let ranges = store.get_ranges(&path, &[0..2, 8..10, 4..4]).await.unwrap();
        assert_eq!(
            ranges,
            vec![Bytes::from("01"), Bytes::from("89"), Bytes::new()]
        );
        let head = store.head(&path).await.unwrap();
        assert_eq!(head.size, 10);
        assert_eq!(head.location, path);

        let uris = unsafe { &*mock }.uris.lock().unwrap().clone();
        assert_eq!(
            uris.len(),
            5,
            "one engine read per get and head, one for the coalesced ranges: {uris:?}"
        );
        assert!(uris
            .iter()
            .all(|u| u == "s3://bucket/warehouse/t1/.hoodie/hoodie.properties"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn get_ranges_reads_each_coalesced_range_once() {
        let data: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let mut mock = MockIo::default();
        mock.files.insert("s3://bucket/t/f".into(), data.clone());
        let (store, mock) = store_over(mock, "s3://bucket");
        let ranges = [0..10, 100..110, 2_500_000..2_500_010, 50..50];

        let got = store.get_ranges(&Path::from("t/f"), &ranges).await.unwrap();

        for (range, bytes) in ranges.iter().zip(&got) {
            assert_eq!(
                bytes.as_ref(),
                &data[range.start as usize..range.end as usize]
            );
        }
        let reads = unsafe { &*mock }.uris.lock().unwrap().len();
        assert_eq!(reads, 2, "ranges within 1 MiB share one read");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_local_root_maps_object_paths_to_plain_paths() {
        let mut mock = MockIo::default();
        mock.files.insert("/tmp/t/f".into(), b"abc".to_vec());
        let (store, mock) = store_over(mock, &root_of("file:///tmp/t"));
        let got = store.get(&Path::from("tmp/t/f")).await.unwrap();
        assert_eq!(got.bytes().await.unwrap().as_ref(), b"abc");
        assert_eq!(*unsafe { &*mock }.uris.lock().unwrap(), vec!["/tmp/t/f"]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn engine_failures_map_to_object_store_errors() {
        let mut mock = MockIo::default();
        mock.files
            .insert("s3://bucket/t/small".into(), b"abc".to_vec());
        mock.errors
            .insert("s3://bucket/t/denied".into(), "HTTP 403 Forbidden".into());
        let (store, _) = store_over(mock, "s3://bucket");

        let missing = store.get(&Path::from("t/missing")).await.unwrap_err();
        assert!(
            matches!(&missing, object_store::Error::NotFound { path, .. } if path == "t/missing"),
            "{missing:?}"
        );
        let missing_head = store.head(&Path::from("t/missing")).await.unwrap_err();
        assert!(matches!(missing_head, object_store::Error::NotFound { .. }));

        let denied = store.get(&Path::from("t/denied")).await.unwrap_err();
        assert!(
            matches!(
                denied,
                object_store::Error::Generic {
                    store: STORE_NAME,
                    ..
                }
            ),
            "{denied:?}"
        );
        assert!(
            denied.to_string().contains("HTTP 403 Forbidden"),
            "{denied}"
        );
        assert!(denied.to_string().contains("t/denied"), "{denied}");

        let past_end = store
            .get_range(&Path::from("t/small"), 5..8)
            .await
            .unwrap_err();
        assert!(
            matches!(
                past_end,
                object_store::Error::Generic {
                    store: STORE_NAME,
                    ..
                }
            ),
            "{past_end:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn listing_and_writes_go_to_the_fallback() {
        let (store, mock) = store_over(MockIo::default(), "s3://bucket");
        let path = Path::from("t/written");
        store.put(&path, Bytes::from("xyz").into()).await.unwrap();
        let listed = store
            .list_with_delimiter(Some(&Path::from("t")))
            .await
            .unwrap();
        assert_eq!(listed.objects.len(), 1);
        store.delete(&path).await.unwrap();
        assert!(unsafe { &*mock }.uris.lock().unwrap().is_empty());
    }

    #[test]
    fn the_context_is_released_once_on_drop_and_without_a_read_callback() {
        let released = Arc::new(AtomicUsize::new(0));
        let mock = MockIo {
            released: Arc::clone(&released),
            ..Default::default()
        };
        let (store, _) = store_over(mock, "s3://bucket");
        drop(store);
        assert_eq!(released.load(Ordering::SeqCst), 1);

        let released = Arc::new(AtomicUsize::new(0));
        let (mut io, _) = mock_file_io(MockIo {
            released: Arc::clone(&released),
            ..Default::default()
        });
        io.read = None;
        assert!(unsafe { FileIo::take(&io) }.is_err());
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }
}
