//! Minimal ZIP (stored) streaming for ClickHouse part directories.
//!
//! Custom rather than using a crate because we need both the writer and reader to
//! work on non-seekable streams (`impl Write` / `impl Read` only — no `Seek`).
//! Since file sizes are known upfront from stat(), the writer populates actual sizes
//! in local headers while still using data descriptors for CRC. This lets the
//! streaming reader know entry boundaries without seeking to the central directory.
//! No existing crate (zip, async_zip, rc-zip) does this — they zero out local header
//! sizes in streaming mode, which breaks streaming reads of stored (uncompressed)
//! entries because there's no end-of-stream marker to detect entry boundaries.
//!
//! Goals:
//! - Stream-friendly writer: writes local headers + data + data descriptors, then central directory.
//! - Stream-friendly reader: reads local headers sequentially and ignores central directory.
//! - Security: prevents ZipSlip / path traversal on extraction.

use anyhow::{Context, Result, anyhow, bail};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

const ZIP_LOCAL_FILE_HEADER_SIG: u32 = 0x0403_4b50;
const ZIP_DATA_DESCRIPTOR_SIG: u32 = 0x0807_4b50;
const ZIP_CENTRAL_DIRECTORY_FILE_HEADER_SIG: u32 = 0x0201_4b50;
const ZIP_END_OF_CENTRAL_DIR_SIG: u32 = 0x0605_4b50;
const ZIP64_END_OF_CENTRAL_DIR_SIG: u32 = 0x0606_4b50;
const ZIP64_END_OF_CENTRAL_DIR_LOCATOR_SIG: u32 = 0x0706_4b50;

const ZIP_VERSION_ZIP64: u16 = 45;
const ZIP_FLAG_DATA_DESCRIPTOR: u16 = 1 << 3;
const ZIP_METHOD_STORED: u16 = 0;

const ZIP64_EXTRA_ID: u16 = 0x0001;

#[derive(Debug, Clone)]
pub struct ZipFileEntry {
    pub abs_path: PathBuf,
    pub rel_path: PathBuf,
    pub size: u64,
}

pub fn collect_files(dir: &Path) -> Result<Vec<ZipFileEntry>> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(dir).follow_links(false) {
        let entry = entry?;
        let ft = entry.file_type();
        if ft.is_symlink() {
            bail!(
                "Symlinks are not supported in part zips: {}",
                entry.path().display()
            );
        }
        if !ft.is_file() {
            continue;
        }

        let abs_path = entry.path().to_path_buf();
        let rel_path = abs_path
            .strip_prefix(dir)
            .with_context(|| {
                format!(
                    "Failed to strip prefix {} from {}",
                    dir.display(),
                    abs_path.display()
                )
            })?
            .to_path_buf();

        validate_rel_path(&rel_path)?;

        let size = entry.metadata()?.len();
        out.push(ZipFileEntry {
            abs_path,
            rel_path,
            size,
        });
    }

    // Stable output for repeatable archives and tests.
    out.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(out)
}

pub fn estimate_zip_size(files: &[ZipFileEntry]) -> u64 {
    // Not exact, but good enough for choosing multipart chunk size.
    // Overhead per file: local header (30) + data descriptor (20) + central header (46) + name bytes + zip64 extras.
    let mut total = 0u64;
    for f in files {
        let name_len = path_to_zip_name(&f.rel_path).map_or(0, |s| s.len() as u64);

        let zip64 = f.size > u64::from(u32::MAX);
        let extra_local = if zip64 { 4 + 16 } else { 0 }; // header+len + 2x u64
        let extra_central = if zip64 { 4 + 24 } else { 0 }; // header+len + 3x u64 (incl offset)

        total += 30 + name_len + extra_local; // local header
        total += f.size; // file data
        total += if zip64 { 24 } else { 16 }; // data descriptor (with sig)
        total += 46 + name_len + extra_central; // central header
    }
    // EOCD (+ optional zip64 records)
    total += 22;
    if files.len() > u16::MAX as usize {
        total += 56 + 20;
    }
    total
}

// buf slicing bounded by min(buf.len(), remaining) and read() return value
#[allow(clippy::indexing_slicing)]
pub fn write_zip<W: Write>(writer: &mut W, files: &[ZipFileEntry]) -> Result<u64> {
    #[derive(Debug)]
    struct CentralEntry {
        name: Vec<u8>,
        crc32: u32,
        size: u64,
        local_header_offset: u64,
        needs_zip64: bool,
    }

    let mut offset: u64 = 0;
    let mut central: Vec<CentralEntry> = Vec::with_capacity(files.len());
    let mut buf = vec![0u8; 4 * 1024 * 1024];

    for f in files {
        let name = path_to_zip_name(&f.rel_path)?;
        let name_bytes = name.into_bytes();

        let needs_zip64 = f.size > u64::from(u32::MAX) || offset > u64::from(u32::MAX);
        let local_header_offset = offset;

        // Local file header
        write_u32(writer, ZIP_LOCAL_FILE_HEADER_SIG)?;
        write_u16(writer, ZIP_VERSION_ZIP64)?;
        write_u16(writer, ZIP_FLAG_DATA_DESCRIPTOR)?;
        write_u16(writer, ZIP_METHOD_STORED)?;
        write_u16(writer, 0)?; // mod time
        write_u16(writer, 0)?; // mod date
        write_u32(writer, 0)?; // CRC32 (in data descriptor)

        if needs_zip64 {
            write_u32(writer, 0xFFFF_FFFF)?; // compressed size
            write_u32(writer, 0xFFFF_FFFF)?; // uncompressed size
        } else {
            write_u32(writer, f.size as u32)?;
            write_u32(writer, f.size as u32)?;
        }

        write_u16(
            writer,
            name_bytes
                .len()
                .try_into()
                .context("ZIP filename too long")?,
        )?;

        let local_extra = if needs_zip64 {
            build_zip64_extra_local(f.size, f.size)
        } else {
            Vec::new()
        };
        write_u16(
            writer,
            local_extra.len().try_into().context("ZIP extra too long")?,
        )?;

        writer.write_all(&name_bytes)?;
        writer.write_all(&local_extra)?;

        offset += 30 + name_bytes.len() as u64 + local_extra.len() as u64;

        // File data (stored)
        let mut file = File::open(&f.abs_path)
            .with_context(|| format!("Failed to open {}", f.abs_path.display()))?;
        let mut hasher = crc32fast::Hasher::new();
        let mut remaining = f.size;
        while remaining > 0 {
            let to_read = (buf.len() as u64).min(remaining) as usize;
            let n = file
                .read(&mut buf[..to_read])
                .with_context(|| format!("Failed to read {}", f.abs_path.display()))?;
            if n == 0 {
                bail!(
                    "Unexpected EOF reading {} (expected {} more bytes)",
                    f.abs_path.display(),
                    remaining
                );
            }
            writer.write_all(&buf[..n])?;
            hasher.update(&buf[..n]);
            offset += n as u64;
            remaining -= n as u64;
        }
        let crc32 = hasher.finalize();

        // Data descriptor
        write_u32(writer, ZIP_DATA_DESCRIPTOR_SIG)?;
        write_u32(writer, crc32)?;
        if needs_zip64 {
            write_u64(writer, f.size)?;
            write_u64(writer, f.size)?;
            offset += 24;
        } else {
            write_u32(writer, f.size as u32)?;
            write_u32(writer, f.size as u32)?;
            offset += 16;
        }

        central.push(CentralEntry {
            name: name_bytes,
            crc32,
            size: f.size,
            local_header_offset,
            needs_zip64,
        });
    }

    let central_dir_offset = offset;

    // Central directory
    for c in &central {
        write_u32(writer, ZIP_CENTRAL_DIRECTORY_FILE_HEADER_SIG)?;
        write_u16(writer, ZIP_VERSION_ZIP64)?; // version made by
        write_u16(writer, ZIP_VERSION_ZIP64)?; // version needed
        write_u16(writer, ZIP_FLAG_DATA_DESCRIPTOR)?;
        write_u16(writer, ZIP_METHOD_STORED)?;
        write_u16(writer, 0)?; // mod time
        write_u16(writer, 0)?; // mod date
        write_u32(writer, c.crc32)?;

        let needs_zip64 = c.needs_zip64 || c.local_header_offset > u64::from(u32::MAX);
        if needs_zip64 {
            write_u32(writer, 0xFFFF_FFFF)?;
            write_u32(writer, 0xFFFF_FFFF)?;
        } else {
            write_u32(writer, c.size as u32)?;
            write_u32(writer, c.size as u32)?;
        }

        write_u16(
            writer,
            c.name.len().try_into().context("ZIP filename too long")?,
        )?;

        let central_extra = if needs_zip64 {
            build_zip64_extra_central(c.size, c.size, c.local_header_offset)
        } else {
            Vec::new()
        };
        write_u16(
            writer,
            central_extra
                .len()
                .try_into()
                .context("ZIP extra too long")?,
        )?;

        write_u16(writer, 0)?; // comment len
        write_u16(writer, 0)?; // disk start
        write_u16(writer, 0)?; // internal attrs
        write_u32(writer, 0)?; // external attrs

        if needs_zip64 {
            write_u32(writer, 0xFFFF_FFFF)?;
        } else {
            write_u32(writer, c.local_header_offset as u32)?;
        }

        writer.write_all(&c.name)?;
        writer.write_all(&central_extra)?;

        offset += 46 + c.name.len() as u64 + central_extra.len() as u64;
    }

    let central_dir_size = offset - central_dir_offset;

    let file_count = central.len() as u64;
    let needs_zip64_eocd = file_count > u64::from(u16::MAX)
        || central_dir_offset > u64::from(u32::MAX)
        || central_dir_size > u64::from(u32::MAX);

    if needs_zip64_eocd {
        // Zip64 End of Central Directory Record
        write_u32(writer, ZIP64_END_OF_CENTRAL_DIR_SIG)?;
        write_u64(writer, 44)?; // size of zip64 eocd record (remaining bytes)
        write_u16(writer, ZIP_VERSION_ZIP64)?;
        write_u16(writer, ZIP_VERSION_ZIP64)?;
        write_u32(writer, 0)?; // disk number
        write_u32(writer, 0)?; // disk with central dir
        write_u64(writer, file_count)?;
        write_u64(writer, file_count)?;
        write_u64(writer, central_dir_size)?;
        write_u64(writer, central_dir_offset)?;
        offset += 56;

        // Zip64 End of Central Directory Locator
        write_u32(writer, ZIP64_END_OF_CENTRAL_DIR_LOCATOR_SIG)?;
        write_u32(writer, 0)?; // disk with zip64 eocd
        write_u64(writer, central_dir_offset + central_dir_size)?; // offset of zip64 eocd
        write_u32(writer, 1)?; // number of disks
        offset += 20;
    }

    // End of Central Directory Record
    write_u32(writer, ZIP_END_OF_CENTRAL_DIR_SIG)?;
    write_u16(writer, 0)?; // disk number
    write_u16(writer, 0)?; // disk with central dir

    if needs_zip64_eocd {
        write_u16(writer, 0xFFFF)?;
        write_u16(writer, 0xFFFF)?;
        write_u32(writer, 0xFFFF_FFFF)?;
        write_u32(writer, 0xFFFF_FFFF)?;
    } else {
        write_u16(writer, file_count as u16)?;
        write_u16(writer, file_count as u16)?;
        write_u32(writer, central_dir_size as u32)?;
        write_u32(writer, central_dir_offset as u32)?;
    }

    write_u16(writer, 0)?; // comment len
    offset += 22;

    Ok(offset)
}

// buf slicing bounded by min(buf.len(), remaining) and read() return value
#[allow(clippy::indexing_slicing)]
pub fn extract_zip<R: Read>(reader: &mut R, dest_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dest_dir)
        .with_context(|| format!("Failed to create {}", dest_dir.display()))?;

    let mut buf = vec![0u8; 1024 * 1024];

    loop {
        let Some(sig) = read_u32_opt(reader)? else {
            break;
        };

        match sig {
            ZIP_LOCAL_FILE_HEADER_SIG => {
                let _version_needed = read_u16(reader)?;
                let flags = read_u16(reader)?;
                let method = read_u16(reader)?;
                let _mod_time = read_u16(reader)?;
                let _mod_date = read_u16(reader)?;
                let header_crc32 = read_u32(reader)?;
                let mut compressed_size = u64::from(read_u32(reader)?);
                let mut uncompressed_size = u64::from(read_u32(reader)?);
                let name_len = read_u16(reader)? as usize;
                let extra_len = read_u16(reader)? as usize;

                let mut name_bytes = vec![0u8; name_len];
                reader
                    .read_exact(&mut name_bytes)
                    .context("Failed to read ZIP filename")?;
                let name = String::from_utf8(name_bytes).context("ZIP filename is not UTF-8")?;

                let mut extra = vec![0u8; extra_len];
                reader
                    .read_exact(&mut extra)
                    .context("Failed to read ZIP extra")?;

                if method != ZIP_METHOD_STORED {
                    bail!("Unsupported ZIP method {method} (only stored supported)");
                }

                let zip64 = compressed_size == 0xFFFF_FFFF || uncompressed_size == 0xFFFF_FFFF;
                if zip64 {
                    let (c, u) = parse_zip64_sizes_from_extra(&extra)?;
                    compressed_size = c;
                    uncompressed_size = u;
                }

                if name.ends_with('/') {
                    // Directory entry (we don't create these, but tolerate them).
                    let rel = PathBuf::from(name.trim_end_matches('/'));
                    validate_rel_path(&rel)?;
                    let dir = safe_join(dest_dir, &rel)?;
                    std::fs::create_dir_all(&dir)?;
                    // No file data expected.
                    continue;
                }

                let rel = PathBuf::from(&name);
                validate_rel_path(&rel)?;
                let out_path = safe_join(dest_dir, &rel)?;
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent)?;
                }

                // Read file data and write to disk, computing CRC.
                let mut out = File::create(&out_path)
                    .with_context(|| format!("Failed to create {}", out_path.display()))?;
                let mut hasher = crc32fast::Hasher::new();
                let mut remaining = compressed_size;
                while remaining > 0 {
                    let to_read = (buf.len() as u64).min(remaining) as usize;
                    let n = reader
                        .read(&mut buf[..to_read])
                        .context("Failed to read ZIP data")?;
                    if n == 0 {
                        bail!("Unexpected EOF reading ZIP data for {name}");
                    }
                    out.write_all(&buf[..n])?;
                    hasher.update(&buf[..n]);
                    remaining -= n as u64;
                }

                // Data descriptor (because writer always sets it).
                let has_dd = (flags & ZIP_FLAG_DATA_DESCRIPTOR) != 0;
                let (crc32, dd_csize, dd_usize) = if has_dd {
                    read_data_descriptor(reader, zip64)?
                } else {
                    (header_crc32, compressed_size, uncompressed_size)
                };

                let computed_crc = hasher.finalize();
                if crc32 != computed_crc {
                    bail!("CRC mismatch for {name}: expected {crc32:08x}, got {computed_crc:08x}");
                }
                if dd_csize != compressed_size || dd_usize != uncompressed_size {
                    bail!(
                        "Size mismatch for {name}: header c/u {compressed_size} / {uncompressed_size}, dd c/u {dd_csize} / {dd_usize}"
                    );
                }
            }
            ZIP_CENTRAL_DIRECTORY_FILE_HEADER_SIG
            | ZIP_END_OF_CENTRAL_DIR_SIG
            | ZIP64_END_OF_CENTRAL_DIR_SIG
            | ZIP64_END_OF_CENTRAL_DIR_LOCATOR_SIG => {
                // We streamed all local entries; stop.
                break;
            }
            other => {
                bail!("Unexpected ZIP signature: 0x{other:08x}");
            }
        }
    }

    Ok(())
}

fn validate_rel_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        bail!("Empty ZIP entry path");
    }
    for comp in path.components() {
        match comp {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                bail!("Unsafe ZIP entry path: {}", path.display())
            }
        }
    }
    Ok(())
}

fn safe_join(base: &Path, rel: &Path) -> Result<PathBuf> {
    validate_rel_path(rel)?;
    Ok(base.join(rel))
}

fn path_to_zip_name(path: &Path) -> Result<String> {
    validate_rel_path(path)?;
    // Normalize to forward slashes.
    let mut parts = Vec::new();
    for comp in path.components() {
        match comp {
            Component::Normal(p) => parts.push(p.to_string_lossy()),
            Component::CurDir => {}
            _ => {
                bail!("Unsupported path component in ZIP name: {}", path.display());
            }
        }
    }
    let s = parts.join("/");
    if s.is_empty() {
        bail!("Empty ZIP entry name");
    }
    if s.contains('\\') {
        bail!("Backslashes not allowed in ZIP entry name: {s}");
    }
    Ok(s)
}

fn build_zip64_extra_local(compressed: u64, uncompressed: u64) -> Vec<u8> {
    // ZIP64 extended information extra field for local header:
    // [id=0x0001][len=16][uncompressed u64][compressed u64]
    let mut v = Vec::with_capacity(4 + 16);
    v.extend_from_slice(&ZIP64_EXTRA_ID.to_le_bytes());
    v.extend_from_slice(&(16u16).to_le_bytes());
    v.extend_from_slice(&uncompressed.to_le_bytes());
    v.extend_from_slice(&compressed.to_le_bytes());
    v
}

fn build_zip64_extra_central(compressed: u64, uncompressed: u64, offset: u64) -> Vec<u8> {
    // Central dir extra can include sizes + local header offset.
    // [id=0x0001][len=24][uncompressed u64][compressed u64][offset u64]
    let mut v = Vec::with_capacity(4 + 24);
    v.extend_from_slice(&ZIP64_EXTRA_ID.to_le_bytes());
    v.extend_from_slice(&(24u16).to_le_bytes());
    v.extend_from_slice(&uncompressed.to_le_bytes());
    v.extend_from_slice(&compressed.to_le_bytes());
    v.extend_from_slice(&offset.to_le_bytes());
    v
}

// Bounds checked: loop guard (i+4<=len), field guard (i+len<=len), and len>=16 check
#[allow(clippy::indexing_slicing, clippy::unwrap_used)]
fn parse_zip64_sizes_from_extra(extra: &[u8]) -> Result<(u64, u64)> {
    let mut i = 0usize;
    while i + 4 <= extra.len() {
        let id = u16::from_le_bytes(extra[i..i + 2].try_into().unwrap());
        let len = u16::from_le_bytes(extra[i + 2..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if i + len > extra.len() {
            break;
        }
        if id == ZIP64_EXTRA_ID {
            if len < 16 {
                bail!("Invalid ZIP64 extra length {len}");
            }
            let uncompressed = u64::from_le_bytes(extra[i..i + 8].try_into().unwrap());
            let compressed = u64::from_le_bytes(extra[i + 8..i + 16].try_into().unwrap());
            return Ok((compressed, uncompressed));
        }
        i += len;
    }
    bail!("ZIP64 sizes missing from extra")
}

fn read_data_descriptor<R: Read>(reader: &mut R, zip64: bool) -> Result<(u32, u64, u64)> {
    let first = read_u32(reader)?;
    let (crc32, csize, usize) = if first == ZIP_DATA_DESCRIPTOR_SIG {
        let crc32 = read_u32(reader)?;
        if zip64 {
            let csize = read_u64(reader)?;
            let usize = read_u64(reader)?;
            (crc32, csize, usize)
        } else {
            let csize = u64::from(read_u32(reader)?);
            let usize = u64::from(read_u32(reader)?);
            (crc32, csize, usize)
        }
    } else {
        // Signature omitted; `first` is CRC32.
        let crc32 = first;
        if zip64 {
            let csize = read_u64(reader)?;
            let usize = read_u64(reader)?;
            (crc32, csize, usize)
        } else {
            let csize = u64::from(read_u32(reader)?);
            let usize = u64::from(read_u32(reader)?);
            (crc32, csize, usize)
        }
    };
    Ok((crc32, csize, usize))
}

fn write_u16<W: Write>(w: &mut W, v: u16) -> Result<()> {
    w.write_all(&v.to_le_bytes())
        .context("Failed to write u16")?;
    Ok(())
}

fn write_u32<W: Write>(w: &mut W, v: u32) -> Result<()> {
    w.write_all(&v.to_le_bytes())
        .context("Failed to write u32")?;
    Ok(())
}

fn write_u64<W: Write>(w: &mut W, v: u64) -> Result<()> {
    w.write_all(&v.to_le_bytes())
        .context("Failed to write u64")?;
    Ok(())
}

fn read_u16<R: Read>(r: &mut R) -> Result<u16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf).context("Failed to read u16")?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf).context("Failed to read u32")?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u32_opt<R: Read>(r: &mut R) -> Result<Option<u32>> {
    let mut buf = [0u8; 4];
    match r.read_exact(&mut buf) {
        Ok(()) => Ok(Some(u32::from_le_bytes(buf))),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
        Err(e) => Err(anyhow!(e)).context("Failed to read u32"),
    }
}

fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf).context("Failed to read u64")?;
    Ok(u64::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn zip_roundtrip() {
        let src = TempDir::new().unwrap();
        std::fs::create_dir_all(src.path().join("sub")).unwrap();
        std::fs::write(src.path().join("a.txt"), b"hello").unwrap();
        std::fs::write(src.path().join("sub").join("b.bin"), b"\x00\x01\x02").unwrap();

        let files = collect_files(src.path()).unwrap();
        let mut buf: Vec<u8> = Vec::new();
        let _size = write_zip(&mut buf, &files).unwrap();

        let dst = TempDir::new().unwrap();
        let mut cursor = std::io::Cursor::new(buf);
        extract_zip(&mut cursor, dst.path()).unwrap();

        assert_eq!(std::fs::read(dst.path().join("a.txt")).unwrap(), b"hello");
        assert_eq!(
            std::fs::read(dst.path().join("sub").join("b.bin")).unwrap(),
            b"\x00\x01\x02"
        );
    }

    #[test]
    fn rejects_zip_slip() {
        // Handcraft a malicious local header with "../evil".
        let mut zip = Vec::new();
        write_u32(&mut zip, ZIP_LOCAL_FILE_HEADER_SIG).unwrap();
        write_u16(&mut zip, ZIP_VERSION_ZIP64).unwrap();
        write_u16(&mut zip, ZIP_FLAG_DATA_DESCRIPTOR).unwrap();
        write_u16(&mut zip, ZIP_METHOD_STORED).unwrap();
        write_u16(&mut zip, 0).unwrap();
        write_u16(&mut zip, 0).unwrap();
        write_u32(&mut zip, 0).unwrap();
        write_u32(&mut zip, 1).unwrap();
        write_u32(&mut zip, 1).unwrap();
        write_u16(&mut zip, 8).unwrap(); // "../evil"
        write_u16(&mut zip, 0).unwrap();
        zip.extend_from_slice(b"../evil");
        zip.push(b'x');
        // data descriptor
        write_u32(&mut zip, ZIP_DATA_DESCRIPTOR_SIG).unwrap();
        write_u32(&mut zip, crc32fast::hash(b"x")).unwrap();
        write_u32(&mut zip, 1).unwrap();
        write_u32(&mut zip, 1).unwrap();

        let dst = TempDir::new().unwrap();
        let mut cursor = std::io::Cursor::new(zip);
        let err = extract_zip(&mut cursor, dst.path()).unwrap_err();
        assert!(format!("{err:#}").contains("Unsafe ZIP entry path"));
    }

    #[test]
    fn estimate_is_nonzero() {
        let src = TempDir::new().unwrap();
        std::fs::write(src.path().join("a"), b"x").unwrap();
        let files = collect_files(src.path()).unwrap();
        assert!(estimate_zip_size(&files) > 0);
    }
}
