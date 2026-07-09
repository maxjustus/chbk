//! Live TUI progress display during backup.
//!
//! When stdout is a PTY, renders a ratatui-based interactive display with
//! per-part status, per-table summary, and keyboard sorting.
//! Falls back to periodic println output when not a terminal.

use crate::util::format_bytes;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{self, IsTerminal};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

// -- Public event types sent by the backup pipeline --

pub struct PartSummary {
    pub hash: String,
    pub database: String,
    pub table: String,
    pub part_name: String,
    pub bytes_on_disk: u64,
    pub rows_count: u64,
}

pub enum BackupEvent {
    /// All parts discovered, with known hashes (already in prior manifest).
    PartsDiscovered {
        parts: Vec<PartSummary>,
        known_hashes: std::collections::HashSet<String>,
    },
    /// A part is being HEAD-checked against S3 before upload.
    PartHeadCheck { hash: String },
    /// A HEAD check found the part already exists in S3; upload skipped.
    PartHeadSkipped { hash: String, size: u64 },
    /// A part is being staged (hardlinked/copied).
    PartStaging { hash: String },
    /// A part has been staged with its file count.
    PartStaged { hash: String, file_count: u32 },
    /// A part ZIP archive is being uploaded.
    PartUploading { hash: String },
    /// Upload progress for a part (sent from multipart upload).
    PartUploadProgress {
        hash: String,
        bytes_uploaded: u64,
        total: u64,
    },
    /// A part has been fully uploaded.
    PartDone { hash: String, zip_size: u64 },
    /// Backup is complete.
    BackupComplete { snapshot_name: String },
}

/// Returns true if stdout is a terminal and TUI mode should be used.
pub fn is_tui_mode() -> bool {
    io::stdout().is_terminal()
}

/// Create a progress channel and spawn the appropriate consumer (TUI or plain).
/// Returns (sender, join_handle, is_tui).
pub fn spawn_progress(
    snapshot_name: &str,
    upload_progress: Option<Arc<crate::storage::UploadProgress>>,
) -> (
    mpsc::UnboundedSender<BackupEvent>,
    tokio::task::JoinHandle<()>,
    bool,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let name = snapshot_name.to_string();
    let tui = is_tui_mode();
    let handle = if tui {
        tokio::spawn(run_tui(rx, name, upload_progress))
    } else {
        tokio::spawn(run_plain_logger(rx, name))
    };
    (tx, handle, tui)
}

// -- Part status tracking --

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PartStatus {
    Skipped = 0, // already in prior manifest
    Pending = 1,
    HeadCheck = 2, // HEAD-checking S3 before upload
    Staging = 3,
    Staged = 4,
    Uploading = 5,
    HeadSkip = 6, // HEAD confirmed exists, upload skipped
    Done = 7,
}

impl PartStatus {
    const fn label(self) -> &'static str {
        match self {
            Self::Skipped => "skipped",
            Self::Pending => "pending",
            Self::Staging => "staging",
            Self::Staged => "staged",
            Self::HeadCheck => "checking",
            Self::Uploading => "uploading",
            Self::HeadSkip => "exists",
            Self::Done => "done",
        }
    }
}

struct PartRow {
    database: String,
    table: String,
    part_name: String,
    bytes_on_disk: u64,
    rows_count: u64,
    file_count: Option<u32>,
    status: PartStatus,
    progress_bytes: u64,
    progress_total: u64,
    is_new: bool,
    zip_size: u64,
    done_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortColumn {
    Database,
    Table,
    PartName,
    Size,
    Status,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatusFilter {
    All,
    New,   // is_new == true
    Skip,  // Skipped | HeadSkip
    Todo,  // Pending | Staged
    Doing, // HeadCheck | Staging | Uploading
    Done,  // Done
}

impl StatusFilter {
    const fn next(self) -> Self {
        match self {
            Self::All => Self::New,
            Self::New => Self::Skip,
            Self::Skip => Self::Todo,
            Self::Todo => Self::Doing,
            Self::Doing => Self::Done,
            Self::Done => Self::All,
        }
    }

    fn matches(self, p: &PartRow) -> bool {
        match self {
            Self::All => true,
            Self::New => p.is_new,
            Self::Skip => matches!(p.status, PartStatus::Skipped | PartStatus::HeadSkip),
            Self::Todo => matches!(p.status, PartStatus::Pending | PartStatus::Staged),
            Self::Doing => matches!(
                p.status,
                PartStatus::HeadCheck | PartStatus::Staging | PartStatus::Uploading
            ),
            Self::Done => p.status == PartStatus::Done,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::New => "New",
            Self::Skip => "Skip",
            Self::Todo => "Todo",
            Self::Doing => "Doing",
            Self::Done => "Done",
        }
    }
}

struct TuiState {
    snapshot_name: String,
    start: Instant,
    parts: Vec<PartRow>,
    part_index: HashMap<String, usize>, // hash -> index
    sort_column: SortColumn,
    sort_ascending: bool,
    status_filter: StatusFilter,
    complete: bool,
}

impl TuiState {
    fn new(snapshot_name: String) -> Self {
        Self {
            snapshot_name,
            start: Instant::now(),
            parts: Vec::new(),
            part_index: HashMap::new(),
            sort_column: SortColumn::Status,
            sort_ascending: false,
            status_filter: StatusFilter::New,
            complete: false,
        }
    }

    // idx comes from self.part_index which is always in sync with self.parts
    #[allow(clippy::indexing_slicing)]
    fn apply_event(&mut self, event: BackupEvent) {
        match event {
            BackupEvent::PartsDiscovered {
                parts,
                known_hashes,
            } => {
                self.parts.clear();
                self.part_index.clear();
                for (i, p) in parts.into_iter().enumerate() {
                    let is_new = !known_hashes.contains(&p.hash);
                    let _ = self.part_index.insert(p.hash, i);
                    self.parts.push(PartRow {
                        database: p.database,
                        table: p.table,
                        part_name: p.part_name,
                        bytes_on_disk: p.bytes_on_disk,
                        rows_count: p.rows_count,
                        file_count: None,
                        status: if is_new {
                            PartStatus::Pending
                        } else {
                            PartStatus::Skipped
                        },
                        progress_bytes: 0,
                        progress_total: 0,
                        is_new,
                        zip_size: 0,
                        done_at: None,
                    });
                }
            }
            BackupEvent::PartHeadCheck { hash } => {
                if let Some(&idx) = self.part_index.get(&hash)
                    && PartStatus::HeadCheck > self.parts[idx].status
                {
                    self.parts[idx].status = PartStatus::HeadCheck;
                }
            }
            BackupEvent::PartHeadSkipped { hash, size } => {
                if let Some(&idx) = self.part_index.get(&hash)
                    && PartStatus::HeadSkip > self.parts[idx].status
                {
                    self.parts[idx].status = PartStatus::HeadSkip;
                    self.parts[idx].zip_size = size;
                    self.parts[idx].done_at = Some(Instant::now());
                }
            }
            BackupEvent::PartStaging { hash } => {
                if let Some(&idx) = self.part_index.get(&hash)
                    && PartStatus::Staging > self.parts[idx].status
                {
                    self.parts[idx].status = PartStatus::Staging;
                }
            }
            BackupEvent::PartStaged { hash, file_count } => {
                if let Some(&idx) = self.part_index.get(&hash)
                    && PartStatus::Staged > self.parts[idx].status
                {
                    self.parts[idx].status = PartStatus::Staged;
                    self.parts[idx].file_count = Some(file_count);
                }
            }
            BackupEvent::PartUploading { hash } => {
                if let Some(&idx) = self.part_index.get(&hash)
                    && PartStatus::Uploading > self.parts[idx].status
                {
                    self.parts[idx].status = PartStatus::Uploading;
                    self.parts[idx].progress_bytes = 0;
                }
            }
            BackupEvent::PartUploadProgress {
                hash,
                bytes_uploaded,
                total,
            } => {
                if let Some(&idx) = self.part_index.get(&hash) {
                    self.parts[idx].progress_bytes = bytes_uploaded;
                    self.parts[idx].progress_total = total;
                }
            }
            BackupEvent::PartDone { hash, zip_size } => {
                if let Some(&idx) = self.part_index.get(&hash)
                    && PartStatus::Done > self.parts[idx].status
                {
                    self.parts[idx].status = PartStatus::Done;
                    self.parts[idx].zip_size = zip_size;
                    self.parts[idx].progress_bytes = zip_size;
                    self.parts[idx].progress_total = zip_size;
                    self.parts[idx].done_at = Some(Instant::now());
                }
            }
            BackupEvent::BackupComplete { .. } => {
                self.complete = true;
            }
        }
    }

    // indices generated from 0..self.parts.len(), always valid
    #[allow(clippy::indexing_slicing)]
    fn sorted_filtered_indices(&self) -> Vec<usize> {
        let mut indices: Vec<usize> = (0..self.parts.len())
            .filter(|&i| self.status_filter.matches(&self.parts[i]))
            .collect();

        indices.sort_by(|&a, &b| {
            let pa = &self.parts[a];
            let pb = &self.parts[b];
            let cmp = match self.sort_column {
                SortColumn::Database => pa.database.cmp(&pb.database).then(pa.table.cmp(&pb.table)),
                SortColumn::Table => pa.table.cmp(&pb.table).then(pa.database.cmp(&pb.database)),
                SortColumn::PartName => pa.part_name.cmp(&pb.part_name),
                SortColumn::Size => pa.bytes_on_disk.cmp(&pb.bytes_on_disk),
                SortColumn::Status => pa.status.cmp(&pb.status),
            };
            if self.sort_ascending {
                cmp
            } else {
                cmp.reverse()
            }
        });
        indices
    }

    fn toggle_sort(&mut self, col: SortColumn) {
        if self.sort_column == col {
            self.sort_ascending = !self.sort_ascending;
        } else {
            self.sort_column = col;
            self.sort_ascending = true;
        }
    }

    fn overall_stats(&self) -> OverallStats {
        let mut stats = OverallStats::default();
        for p in &self.parts {
            stats.total += 1;
            stats.total_bytes += p.bytes_on_disk;
            if p.is_new {
                stats.new_parts += 1;
            }
            match p.status {
                PartStatus::Skipped => {
                    stats.uploaded_bytes += p.bytes_on_disk;
                }
                PartStatus::Done => {
                    stats.uploaded += 1;
                    stats.uploaded_bytes += p.zip_size;
                }
                PartStatus::Uploading => {
                    stats.in_progress += 1;
                    stats.uploaded_bytes += p.progress_bytes;
                }
                PartStatus::Staged => stats.staged += 1,
                PartStatus::Staging => stats.staging += 1,
                PartStatus::HeadCheck => stats.in_progress += 1,
                PartStatus::HeadSkip => {
                    stats.head_skipped += 1;
                    stats.uploaded += 1;
                    stats.uploaded_bytes += p.zip_size;
                }
                PartStatus::Pending => {}
            }
        }
        stats
    }

    fn table_summary(&self) -> Vec<TableSummaryRow> {
        let mut map: HashMap<(String, String), TableSummaryRow> = HashMap::new();
        for p in &self.parts {
            let key = (p.database.clone(), p.table.clone());
            let row = map.entry(key).or_insert_with(|| TableSummaryRow {
                database: p.database.clone(),
                table: p.table.clone(),
                parts: 0,
                skip: 0,
                todo: 0,
                doing: 0,
                done: 0,
                bytes_on_disk: 0,
                rows_count: 0,
                file_count: 0,
            });
            row.parts += 1;
            match p.status {
                PartStatus::Skipped | PartStatus::HeadSkip => row.skip += 1,
                PartStatus::Pending | PartStatus::Staged => row.todo += 1,
                PartStatus::HeadCheck | PartStatus::Staging | PartStatus::Uploading => {
                    row.doing += 1;
                }
                PartStatus::Done => row.done += 1,
            }
            row.bytes_on_disk += p.bytes_on_disk;
            row.rows_count += p.rows_count;
            if let Some(fc) = p.file_count {
                row.file_count += fc;
            }
        }
        let mut rows: Vec<_> = map.into_values().collect();
        rows.sort_by(|a, b| a.database.cmp(&b.database).then(a.table.cmp(&b.table)));
        rows
    }
}

#[derive(Default)]
struct OverallStats {
    total: u64,
    new_parts: u64,
    staging: u64,
    staged: u64,
    in_progress: u64,
    uploaded: u64,
    head_skipped: u64,
    total_bytes: u64,
    uploaded_bytes: u64,
}

struct TableSummaryRow {
    database: String,
    table: String,
    parts: u32,
    skip: u32,
    todo: u32,
    doing: u32,
    done: u32,
    bytes_on_disk: u64,
    rows_count: u64,
    file_count: u32,
}

// -- ratatui TUI --

use crossterm::event::{self as ct_event, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState};

async fn run_tui(
    mut rx: mpsc::UnboundedReceiver<BackupEvent>,
    snapshot_name: String,
    upload_progress: Option<Arc<crate::storage::UploadProgress>>,
) {
    if let Err(e) = run_tui_inner(&mut rx, snapshot_name, upload_progress).await {
        eprintln!("TUI error: {e}");
        // Drain remaining events so the sender doesn't block
        while rx.recv().await.is_some() {}
    }
}

// Layout indexing safe: splits produce exactly 4 chunks
#[allow(clippy::indexing_slicing, clippy::unwrap_used)]
async fn run_tui_inner(
    rx: &mut mpsc::UnboundedReceiver<BackupEvent>,
    snapshot_name: String,
    upload_progress: Option<Arc<crate::storage::UploadProgress>>,
) -> anyhow::Result<()> {
    enable_raw_mode()?;
    crossterm::execute!(io::stdout(), EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let mut state = TuiState::new(snapshot_name);
    let mut table_state = TableState::default();
    let mut rate_samples: std::collections::VecDeque<(Instant, u64)> =
        std::collections::VecDeque::new();
    let mut last_rate_check = Instant::now();
    let mut upload_rate: u64 = 0;

    loop {
        // Drain pending events (non-blocking), capped per frame so
        // fast bursts (e.g. HEAD verification) remain visible.
        const MAX_EVENTS_PER_FRAME: usize = 32;
        for _ in 0..MAX_EVENTS_PER_FRAME {
            match rx.try_recv() {
                Ok(event) => state.apply_event(event),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    state.complete = true;
                    break;
                }
            }
        }

        // Calculate upload rate from atomic counters (no event-queue lag)
        let now = Instant::now();
        let stats = state.overall_stats();
        if now.duration_since(last_rate_check) >= Duration::from_secs(1) {
            let current_bytes = upload_progress.as_ref().map_or(
                stats.uploaded_bytes,
                |p: &Arc<crate::storage::UploadProgress>| p.uploaded_bytes(),
            );
            rate_samples.push_back((now, current_bytes));
            while rate_samples.len() > 5 {
                let _ = rate_samples.pop_front();
            }
            if rate_samples.len() >= 2 {
                let (oldest_t, oldest_b) = *rate_samples.front().unwrap();
                let delta_b = current_bytes.saturating_sub(oldest_b);
                let delta_s = now.duration_since(oldest_t).as_secs_f64();
                upload_rate = if delta_s > 0.0 {
                    (delta_b as f64 / delta_s) as u64
                } else {
                    0
                };
            }
            last_rate_check = now;
        }

        let elapsed = state.start.elapsed();
        let sorted = state.sorted_filtered_indices();

        // Clamp selection if filtered set shrank (parts moved between statuses).
        if let Some(sel) = table_state.selected()
            && sel >= sorted.len()
        {
            table_state.select(if sorted.is_empty() {
                None
            } else {
                Some(sorted.len() - 1)
            });
        }

        // Draw
        let _ = terminal.draw(|f| {
            let area = f.area();
            let chunks = Layout::vertical([
                Constraint::Length(3), // header
                Constraint::Min(5),    // tables summary
                Constraint::Min(10),   // parts detail
                Constraint::Length(1), // footer
            ])
            .split(area);

            draw_header(
                f,
                chunks[0],
                &state.snapshot_name,
                &stats,
                upload_rate,
                elapsed,
                upload_progress.as_ref(),
            );
            draw_table_summary(f, chunks[1], &state.table_summary());
            draw_parts_detail(f, chunks[2], &state, &sorted, &mut table_state);
            draw_footer(f, chunks[3], &stats, &state);
        })?;

        // Compute page size for PageUp/Down from the parts detail panel height.
        let parts_page_size = {
            let size = terminal.size()?;
            let area = Rect::new(0, 0, size.width, size.height);
            let layout = Layout::vertical([
                Constraint::Length(3),
                Constraint::Min(5),
                Constraint::Min(10),
                Constraint::Length(1),
            ])
            .split(area);
            layout[2].height.saturating_sub(3) as usize // borders + header
        };

        // Handle input (poll with timeout for ~30fps)
        if ct_event::poll(Duration::from_millis(33))?
            && let Event::Key(key) = ct_event::read()?
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                KeyCode::Char('d') => state.toggle_sort(SortColumn::Database),
                KeyCode::Char('t') => state.toggle_sort(SortColumn::Table),
                KeyCode::Char('n') => state.toggle_sort(SortColumn::PartName),
                KeyCode::Char('s') => state.toggle_sort(SortColumn::Size),
                KeyCode::Char('S') => state.toggle_sort(SortColumn::Status),
                KeyCode::Down | KeyCode::Char('j') => {
                    let i = table_state.selected().unwrap_or(0);
                    if i + 1 < sorted.len() {
                        table_state.select(Some(i + 1));
                    }
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    let i = table_state.selected().unwrap_or(0);
                    if i > 0 {
                        table_state.select(Some(i - 1));
                    }
                }
                KeyCode::PageDown => {
                    let i = table_state.selected().unwrap_or(0);
                    let new_i = (i + parts_page_size).min(sorted.len().saturating_sub(1));
                    table_state.select(Some(new_i));
                }
                KeyCode::PageUp => {
                    let i = table_state.selected().unwrap_or(0);
                    table_state.select(Some(i.saturating_sub(parts_page_size)));
                }
                KeyCode::Home => {
                    table_state.select(Some(0));
                }
                KeyCode::End if !sorted.is_empty() => {
                    table_state.select(Some(sorted.len() - 1));
                }
                KeyCode::Tab => {
                    state.status_filter = state.status_filter.next();
                    table_state.select(Some(0));
                }
                _ => {}
            }
        }

        if state.complete {
            // Give user a moment to see final state
            tokio::time::sleep(Duration::from_millis(500)).await;
            break;
        }
    }

    disable_raw_mode()?;
    crossterm::execute!(io::stdout(), LeaveAlternateScreen)?;

    Ok(())
}

fn draw_header(
    f: &mut ratatui::Frame,
    area: Rect,
    snapshot_name: &str,
    stats: &OverallStats,
    rate: u64,
    elapsed: Duration,
    upload_progress: Option<&Arc<crate::storage::UploadProgress>>,
) {
    let elapsed_str = format!("{}m {:02}s", elapsed.as_secs() / 60, elapsed.as_secs() % 60);
    let skip_str = if stats.head_skipped > 0 {
        format!(" | {} already in S3", stats.head_skipped)
    } else {
        String::new()
    };
    let retry_str = if let Some(p) = upload_progress {
        let retries = p.retries();
        let throttles = p.throttles();
        let errors = p.errors();
        if retries > 0 || errors > 0 {
            let mut s = format!(" | {retries} retries");
            if throttles > 0 {
                let _ = write!(s, " ({throttles} throttled)");
            }
            if errors > 0 {
                let _ = write!(s, " {errors} errors");
            }
            s
        } else {
            String::new()
        }
    } else {
        String::new()
    };
    let header_text = format!(
        " Parts: {}/{} uploaded | {} / {} | {}/s | {}{}{}",
        stats.uploaded,
        stats.new_parts,
        format_bytes(stats.uploaded_bytes),
        format_bytes(stats.total_bytes),
        format_bytes(rate),
        elapsed_str,
        skip_str,
        retry_str,
    );

    let block = Block::default()
        .title(format!(" chbak -- {snapshot_name} "))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    let paragraph = Paragraph::new(header_text).block(block);
    f.render_widget(paragraph, area);
}

fn draw_table_summary(f: &mut ratatui::Frame, area: Rect, tables: &[TableSummaryRow]) {
    let header_cells = [
        "Database", "Table", "Parts", "Skip", "Todo", "Doing", "Done", "Size", "Rows", "Files",
    ]
    .iter()
    .map(|h| Cell::from(*h).style(Style::default().add_modifier(Modifier::BOLD)));
    let header = Row::new(header_cells).height(1);

    let mut total_parts: u32 = 0;
    let mut total_skip: u32 = 0;
    let mut total_todo: u32 = 0;
    let mut total_doing: u32 = 0;
    let mut total_done: u32 = 0;
    let mut total_bytes: u64 = 0;
    let mut total_rows: u64 = 0;
    let mut total_files: u32 = 0;

    let rows: Vec<Row> = tables
        .iter()
        .map(|t| {
            total_parts += t.parts;
            total_skip += t.skip;
            total_todo += t.todo;
            total_doing += t.doing;
            total_done += t.done;
            total_bytes += t.bytes_on_disk;
            total_rows += t.rows_count;
            total_files += t.file_count;
            Row::new(vec![
                Cell::from(t.database.as_str()),
                Cell::from(t.table.as_str()),
                Cell::from(format!("{}", t.parts)),
                Cell::from(format!("{}", t.skip)),
                Cell::from(if t.todo > 0 {
                    format!("{}", t.todo)
                } else {
                    String::new()
                }),
                Cell::from(if t.doing > 0 {
                    format!("{}", t.doing)
                } else {
                    String::new()
                })
                .style(Style::default().fg(Color::Yellow)),
                Cell::from(if t.done > 0 {
                    format!("{}", t.done)
                } else {
                    String::new()
                })
                .style(Style::default().fg(Color::Green)),
                Cell::from(format_bytes(t.bytes_on_disk)),
                Cell::from(format_rows(t.rows_count)),
                Cell::from(format!("{}", t.file_count)),
            ])
        })
        .collect();

    let dim = Style::default().add_modifier(Modifier::DIM);
    let total_row = Row::new(vec![
        Cell::from("(total)").style(dim),
        Cell::from(""),
        Cell::from(format!("{total_parts}")).style(dim),
        Cell::from(format!("{total_skip}")).style(dim),
        Cell::from(if total_todo > 0 {
            format!("{total_todo}")
        } else {
            String::new()
        })
        .style(dim),
        Cell::from(if total_doing > 0 {
            format!("{total_doing}")
        } else {
            String::new()
        })
        .style(dim),
        Cell::from(if total_done > 0 {
            format!("{total_done}")
        } else {
            String::new()
        })
        .style(dim),
        Cell::from(format_bytes(total_bytes)).style(dim),
        Cell::from(format_rows(total_rows)).style(dim),
        Cell::from(format!("{total_files}")).style(dim),
    ]);

    let mut all_rows = rows;
    all_rows.push(total_row);

    let widths = [
        Constraint::Min(12),
        Constraint::Min(14),
        Constraint::Length(7),
        Constraint::Length(6),
        Constraint::Length(6),
        Constraint::Length(7),
        Constraint::Length(6),
        Constraint::Length(10),
        Constraint::Length(8),
        Constraint::Length(7),
    ];

    let table = Table::new(all_rows, widths).header(header).block(
        Block::default()
            .title(" Tables ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray)),
    );

    f.render_widget(table, area);
}

// sorted indices come from sorted_filtered_indices(), always valid
#[allow(clippy::indexing_slicing)]
fn draw_parts_detail(
    f: &mut ratatui::Frame,
    area: Rect,
    state: &TuiState,
    sorted: &[usize],
    table_state: &mut TableState,
) {
    let sort_indicator = |col: SortColumn| -> &'static str {
        if state.sort_column == col {
            if state.sort_ascending { " ^" } else { " v" }
        } else {
            ""
        }
    };

    let header_cells = [
        format!("Database{}", sort_indicator(SortColumn::Database)),
        format!("Table{}", sort_indicator(SortColumn::Table)),
        format!("Part{}", sort_indicator(SortColumn::PartName)),
        format!("Size{}", sort_indicator(SortColumn::Size)),
        "Files".to_string(),
        format!("Status{}", sort_indicator(SortColumn::Status)),
        "Done at".to_string(),
    ];
    let header = Row::new(
        header_cells
            .iter()
            .map(|h| Cell::from(h.as_str()).style(Style::default().add_modifier(Modifier::BOLD))),
    )
    .height(1);

    let rows: Vec<Row> = sorted
        .iter()
        .map(|&idx| {
            let p = &state.parts[idx];
            let status_str = status_display(p);
            let status_style = status_color(p.status);
            let done_str = match p.done_at {
                Some(t) => {
                    let elapsed = t.duration_since(state.start);
                    format!("{}:{:02}", elapsed.as_secs() / 60, elapsed.as_secs() % 60)
                }
                None => String::new(),
            };
            Row::new(vec![
                Cell::from(p.database.as_str()),
                Cell::from(p.table.as_str()),
                Cell::from(p.part_name.as_str()),
                Cell::from(format_bytes(p.bytes_on_disk)),
                Cell::from(
                    p.file_count
                        .map_or_else(|| "-".to_string(), |c| format!("{c}")),
                ),
                Cell::from(status_str).style(status_style),
                Cell::from(done_str).style(Style::default().fg(Color::DarkGray)),
            ])
        })
        .collect();

    let pos = table_state.selected().map_or(0, |i| i + 1);
    let title = format!(
        " {} parts ({}/{})  [d]b [t]able [n]ame [s]ize [S]tatus  Tab: filter ",
        state.status_filter.label(),
        pos,
        sorted.len()
    );

    let widths = [
        Constraint::Min(12),
        Constraint::Min(16),
        Constraint::Min(24),
        Constraint::Length(10),
        Constraint::Length(7),
        Constraint::Min(26),
        Constraint::Length(8),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray)),
        )
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    f.render_stateful_widget(table, area, table_state);
}

fn draw_footer(f: &mut ratatui::Frame, area: Rect, stats: &OverallStats, state: &TuiState) {
    let status = if state.complete {
        "complete".to_string()
    } else {
        format!(
            "stage {}/{} | zip+upload {}/{} | q: quit",
            stats.staging + stats.staged + stats.in_progress + stats.uploaded,
            stats.new_parts,
            stats.uploaded,
            stats.new_parts,
        )
    };
    let footer = Paragraph::new(Line::from(vec![Span::styled(
        format!(" {status} "),
        Style::default().fg(Color::DarkGray),
    )]));
    f.render_widget(footer, area);
}

fn progress_bar(bytes: u64, total: u64, bar_width: usize) -> String {
    let pct = (bytes * 100 / total).min(100);
    let filled = (pct as usize * bar_width / 100).min(bar_width);
    let empty = bar_width - filled;
    format!(
        "{}{}  {}%",
        "\u{2593}".repeat(filled),
        "\u{2591}".repeat(empty),
        pct
    )
}

fn status_display(p: &PartRow) -> String {
    match p.status {
        PartStatus::Uploading => {
            if p.progress_total > 0 {
                format!("up:{}", progress_bar(p.progress_bytes, p.progress_total, 6))
            } else {
                "uploading".to_string()
            }
        }
        _ => p.status.label().to_string(),
    }
}

fn status_color(status: PartStatus) -> Style {
    match status {
        PartStatus::Done => Style::default().fg(Color::Green),
        PartStatus::Uploading => Style::default().fg(Color::Yellow),
        PartStatus::Staging | PartStatus::Staged | PartStatus::HeadCheck => {
            Style::default().fg(Color::Blue)
        }
        PartStatus::HeadSkip => Style::default().fg(Color::Cyan),
        PartStatus::Pending | PartStatus::Skipped => Style::default().fg(Color::DarkGray),
    }
}

fn format_rows(count: u64) -> String {
    if count >= 1_000_000_000 {
        format!("{:.1}B", count as f64 / 1_000_000_000.0)
    } else if count >= 1_000_000 {
        format!("{:.1}M", count as f64 / 1_000_000.0)
    } else if count >= 1_000 {
        format!("{:.1}K", count as f64 / 1_000.0)
    } else {
        format!("{count}")
    }
}

// -- Plain text fallback --

async fn run_plain_logger(mut rx: mpsc::UnboundedReceiver<BackupEvent>, _snapshot_name: String) {
    let start = Instant::now();
    let mut new_parts: u64 = 0;
    let mut staged: u64 = 0;
    let mut uploaded: u64 = 0;
    let mut uploaded_bytes: u64 = 0;
    let mut last_log = Instant::now();
    let mut last_bytes: u64 = 0;

    while let Some(event) = rx.recv().await {
        match event {
            BackupEvent::PartsDiscovered {
                parts,
                known_hashes,
            } => {
                let total = parts.len() as u64;
                new_parts = parts
                    .iter()
                    .filter(|p| !known_hashes.contains(&p.hash))
                    .count() as u64;
                let total_bytes: u64 = parts.iter().map(|p| p.bytes_on_disk).sum();
                println!(
                    "Parts: {} total ({} new, {} known), {}",
                    total,
                    new_parts,
                    total - new_parts,
                    format_bytes(total_bytes)
                );
            }
            BackupEvent::PartHeadSkipped { size, .. } => {
                new_parts = new_parts.saturating_sub(1);
                uploaded += 1;
                uploaded_bytes += size;
            }
            BackupEvent::PartStaged { .. } => {
                staged += 1;
            }
            BackupEvent::PartDone { zip_size, .. } => {
                uploaded += 1;
                uploaded_bytes += zip_size;
            }
            BackupEvent::PartUploadProgress { bytes_uploaded, .. } => {
                // Throttled logging
                let now = Instant::now();
                if now.duration_since(last_log) >= Duration::from_secs(1) {
                    let delta = uploaded_bytes.saturating_sub(last_bytes)
                        + bytes_uploaded.saturating_sub(last_bytes);
                    let rate = (delta as f64 / now.duration_since(last_log).as_secs_f64()) as u64;
                    println!(
                        "Progress: staged {}/{} | uploaded {}/{} {} @ {}/s",
                        staged,
                        new_parts,
                        uploaded,
                        new_parts,
                        format_bytes(uploaded_bytes),
                        format_bytes(rate)
                    );
                    last_log = now;
                    last_bytes = uploaded_bytes;
                }
            }
            BackupEvent::BackupComplete { snapshot_name } => {
                let elapsed = start.elapsed();
                println!(
                    "Backup complete: '{}' ({} parts, {}) in {}m {:02}s",
                    snapshot_name,
                    uploaded,
                    format_bytes(uploaded_bytes),
                    elapsed.as_secs() / 60,
                    elapsed.as_secs() % 60
                );
            }
            _ => {}
        }
    }
}
