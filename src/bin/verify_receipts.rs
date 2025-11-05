use anyhow::{Context, Result};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use serde::Deserialize;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tar::Archive;

/// Verify receipt integrity in blockchain data archives
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input directory containing tar.zst files
    #[arg(short, long, default_value = "blocks")]
    input_dir: PathBuf,
}

#[derive(Debug, Deserialize)]
struct BlockData {
    #[serde(default)]
    system_txs: Vec<SystemTx>,
}

#[derive(Debug, Deserialize)]
struct SystemTx {
    receipt: Option<serde::de::IgnoredAny>,
}

#[derive(Default)]
struct Stats {
    total_blocks: AtomicU64,
    total_system_txs: AtomicU64,
    blocks_with_system_txs: AtomicU64,
    null_receipts: AtomicU64,
    corrupted_blocks: AtomicU64,
    failed_blocks: Mutex<Vec<String>>,
    corrupted_block_list: Mutex<Vec<String>>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let blocks_dir = args.input_dir;

    if !blocks_dir.exists() {
        anyhow::bail!("Input directory '{}' not found", blocks_dir.display());
    }

    // Collect all tar.zst files
    let mut tar_files: Vec<_> = std::fs::read_dir(&blocks_dir)
        .context("Failed to read blocks directory")?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.path().extension().and_then(|s| s.to_str()) == Some("zst")
                && entry.path().file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.ends_with(".tar"))
                    .unwrap_or(false)
        })
        .map(|entry| entry.path())
        .collect();

    tar_files.sort();

    println!("Found {} tar.zst files to verify", tar_files.len());
    println!("Starting parallel verification...\n");

    let pb = ProgressBar::new(tar_files.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files ({percent}%) ETA: {eta}")
            .expect("Failed to set progress bar template")
            .progress_chars("#>-")
    );

    let stats = Stats::default();

    // Process files in parallel
    tar_files.par_iter().for_each(|tar_file| {
        if let Err(e) = process_tar_file(tar_file, &stats) {
            pb.println(format!("⚠️  Error processing {}: {}", tar_file.display(), e));
        }
        pb.inc(1);
    });

    pb.finish_with_message("Verification complete!");

    // Print statistics
    let total_blocks = stats.total_blocks.load(Ordering::Relaxed);
    let total_system_txs = stats.total_system_txs.load(Ordering::Relaxed);
    let blocks_with_system_txs = stats.blocks_with_system_txs.load(Ordering::Relaxed);
    let null_receipts = stats.null_receipts.load(Ordering::Relaxed);
    let corrupted_blocks = stats.corrupted_blocks.load(Ordering::Relaxed);
    let failed_blocks = stats.failed_blocks.lock().unwrap();
    let corrupted_block_list = stats.corrupted_block_list.lock().unwrap();

    println!("\n=== VERIFICATION RESULTS ===");
    println!("Total blocks verified:         {}", total_blocks);
    println!("Blocks with system_txs:        {}", blocks_with_system_txs);
    println!("Total system_txs found:        {}", total_system_txs);
    println!("System_txs with null receipts: {}", null_receipts);
    println!("Corrupted/unreadable blocks:   {}", corrupted_blocks);

    if !corrupted_block_list.is_empty() {
        println!("\n⚠️  Corrupted blocks ({}):", corrupted_block_list.len());
        for block_path in corrupted_block_list.iter() {
            println!("  {}", block_path);
        }
    }

    if !failed_blocks.is_empty() {
        println!("\n⚠️  Blocks with null receipts ({}):", failed_blocks.len());
        for block_path in failed_blocks.iter() {
            println!("  {}", block_path);
        }
        anyhow::bail!("Verification failed: {} blocks have null receipts", failed_blocks.len());
    } else {
        println!("\n✅ All system_txs have non-null receipts!");
    }

    Ok(())
}

fn process_tar_file(tar_file: &PathBuf, stats: &Stats) -> Result<()> {
    let file = std::fs::File::open(tar_file)
        .context(format!("Failed to open {}", tar_file.display()))?;

    // Decompress the entire zstd file at once
    let mut zstd_decoder = zstd::stream::read::Decoder::new(file)
        .context("Failed to create zstd decoder")?;
    let mut decompressed_tar = Vec::new();
    zstd_decoder.read_to_end(&mut decompressed_tar)
        .context("Failed to decompress zstd")?;

    // Now read the tar archive from the decompressed data
    let mut archive = Archive::new(&decompressed_tar[..]);

    for entry in archive.entries().context("Failed to read archive entries")? {
        let mut entry = entry.context("Failed to read entry")?;

        let path = entry.path()
            .context("Failed to get entry path")?
            .to_string_lossy()
            .to_string();

        // Only process .rmp.lz4 files
        if !path.ends_with(".rmp.lz4") {
            continue;
        }

        // Read compressed data
        let mut compressed_data = Vec::new();
        if let Err(e) = entry.read_to_end(&mut compressed_data) {
            stats.corrupted_blocks.fetch_add(1, Ordering::Relaxed);
            stats.corrupted_block_list.lock().unwrap().push(format!("{} (read error: {})", path, e));
            continue;
        }

        // Decompress lz4 frame format
        let mut decoder = lz4_flex::frame::FrameDecoder::new(&compressed_data[..]);
        let mut decompressed = Vec::new();
        if let Err(e) = decoder.read_to_end(&mut decompressed) {
            stats.corrupted_blocks.fetch_add(1, Ordering::Relaxed);
            stats.corrupted_block_list.lock().unwrap().push(format!("{} (lz4 error: {})", path, e));
            continue;
        }

        // Deserialize msgpack - it's a list with one element
        let blocks: Vec<BlockData> = match rmp_serde::from_slice(&decompressed) {
            Ok(blocks) => blocks,
            Err(e) => {
                stats.corrupted_blocks.fetch_add(1, Ordering::Relaxed);
                stats.corrupted_block_list.lock().unwrap().push(format!("{} (msgpack error: {})", path, e));
                continue;
            }
        };

        if blocks.is_empty() {
            continue;
        }

        let block_data = &blocks[0];
        stats.total_blocks.fetch_add(1, Ordering::Relaxed);

        if block_data.system_txs.is_empty() {
            continue;
        }

        stats.blocks_with_system_txs.fetch_add(1, Ordering::Relaxed);

        // Check system_txs receipts
        let mut has_null_receipt = false;
        for system_tx in &block_data.system_txs {
            stats.total_system_txs.fetch_add(1, Ordering::Relaxed);
            if system_tx.receipt.is_none() {
                has_null_receipt = true;
                stats.null_receipts.fetch_add(1, Ordering::Relaxed);
            }
        }

        if has_null_receipt {
            stats.failed_blocks.lock().unwrap().push(path);
        }
    }

    Ok(())
}
