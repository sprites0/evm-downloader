use alloy_provider::{Provider, ProviderBuilder};
use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use clap::Parser;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::path::PathBuf;
use tar::{Archive, Builder};
use tokio::sync::mpsc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about = "Fast S3 downloader with on-the-fly tar.zst compression", long_about = None)]
struct Args {
    /// S3 bucket name
    #[arg(short, long, default_value = "hl-mainnet-evm-blocks")]
    bucket: String,

    /// Starting block number (inclusive)
    #[arg(long)]
    start_block: u64,

    /// Ending block number (inclusive). If not specified, fetches latest from RPC
    #[arg(long)]
    end_block: Option<u64>,

    /// RPC endpoint to fetch latest block (used if end_block not specified)
    #[arg(long, default_value = "https://rpc.hyperliquid.xyz/evm")]
    rpc_url: String,

    /// Output directory for checkpoint tar.zst files
    #[arg(short, long, default_value = "blocks")]
    output_dir: PathBuf,

    /// AWS region
    #[arg(short, long, default_value = "ap-northeast-1")]
    region: String,

    /// Use request payer (for requester-pays buckets)
    #[arg(long, default_value = "true")]
    request_payer: bool,

    /// Compression level (1-22, default: 3)
    #[arg(short, long, default_value = "3")]
    compression_level: i32,

    /// Maximum number of concurrent downloads
    #[arg(long, default_value = "100")]
    concurrency: usize,
}

/// Generates the RMP file path for a given block height
/// Format: {f}/{s}/{height}.rmp.lz4
/// where f = ((height - 1) / 1_000_000) * 1_000_000
///       s = ((height - 1) / 1_000) * 1_000
fn rmp_path(height: u64) -> String {
    let f = ((height - 1) / 1_000_000) * 1_000_000;
    let s = ((height - 1) / 1_000) * 1_000;
    format!("{f}/{s}/{height}.rmp.lz4")
}

/// Get the 's' value (1000-block chunk identifier) for a block
fn get_chunk_id(height: u64) -> u64 {
    ((height - 1) / 1_000) * 1_000
}

struct DownloadedBlock {
    chunk_id: u64,
    block_num: u64,
    key: String,
    data: Vec<u8>,
}

struct ExtractedBlock {
    path: String,
    data: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Fetch end_block from RPC if not specified
    let end_block = if let Some(end) = args.end_block {
        end
    } else {
        info!("Fetching latest block number from {}", args.rpc_url);
        let provider = ProviderBuilder::new()
            .connect_http(args.rpc_url.parse().context("Failed to parse RPC URL")?);
        let block_number = provider
            .get_block_number()
            .await
            .context("Failed to fetch latest block number from RPC")?;
        info!("Latest block number: {}", block_number);
        block_number
    };

    info!(
        "Downloading blocks {} to {} from s3://{}/",
        args.start_block, end_block, args.bucket
    );

    // Validate range
    if args.start_block > end_block {
        anyhow::bail!("start_block ({}) must be <= end_block ({})", args.start_block, end_block);
    }

    // Create output directory and .completed directory
    std::fs::create_dir_all(&args.output_dir)
        .context("Failed to create output directory")?;

    let completed_dir = args.output_dir.join(".completed");
    std::fs::create_dir_all(&completed_dir)
        .context("Failed to create .completed directory")?;

    // Load AWS config
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(args.region.clone()))
        .load()
        .await;

    let client = Client::new(&config);

    // Group blocks by chunk (s value)
    let mut chunks: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for block_num in args.start_block..=end_block {
        let chunk_id = get_chunk_id(block_num);
        chunks.entry(chunk_id).or_insert_with(Vec::new).push(block_num);
    }

    // Filter out fully completed chunks, but redownload partial chunks
    let mut chunks_to_download: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    let mut skipped_blocks = 0u64;

    for (chunk_id, blocks) in chunks {
        let completed_marker = completed_dir.join(chunk_id.to_string());
        let partial_marker = completed_dir.join(format!("{}.partial", chunk_id));
        let chunk_file = args.output_dir.join(format!("{}.tar.zst", chunk_id));

        if completed_marker.exists() {
            // Verify the actual chunk file exists
            if chunk_file.exists() {
                // Only skip fully completed chunks with existing files
                info!("Skipping chunk {} (already completed, {} blocks)", chunk_id, blocks.len());
                skipped_blocks += blocks.len() as u64;
            } else {
                // Orphaned marker - file is missing, redownload
                info!("Redownloading chunk {} (marker exists but file is missing)", chunk_id);
                let _ = std::fs::remove_file(&completed_marker);
                chunks_to_download.insert(chunk_id, blocks);
            }
        } else if partial_marker.exists() {
            // Partial chunk - redownload to get missing blocks
            info!("Redownloading chunk {} (previously partial, now has {} blocks in range)", chunk_id, blocks.len());

            // Clean up old partial marker and chunk file
            let _ = std::fs::remove_file(&partial_marker);
            if chunk_file.exists() {
                let _ = std::fs::remove_file(&chunk_file);
            }

            chunks_to_download.insert(chunk_id, blocks);
        } else {
            chunks_to_download.insert(chunk_id, blocks);
        }
    }

    let total_blocks = end_block - args.start_block + 1;
    let blocks_to_download: u64 = chunks_to_download.values().map(|blocks| blocks.len() as u64).sum();

    info!(
        "Total chunks: {}, Chunks to download: {}, Blocks to download: {}/{} ({} skipped)",
        chunks_to_download.len() + (skipped_blocks / 1000) as usize,
        chunks_to_download.len(),
        blocks_to_download,
        total_blocks,
        skipped_blocks
    );

    if chunks_to_download.is_empty() {
        info!("All chunks already downloaded!");
        return Ok(());
    }

    // Create progress bar for total blocks to download
    let pb = ProgressBar::new(blocks_to_download);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {per_sec} ETA: {eta}")
            .expect("Failed to set progress bar template")
            .progress_chars("#>-")
    );

    // Create channel for downloaded blocks
    let (tx, rx) = mpsc::channel::<DownloadedBlock>(5000);

    // Spawn writer task
    let output_dir = args.output_dir.clone();
    let compression_level = args.compression_level;
    let pb_clone = pb.clone();
    let chunks_info = chunks_to_download.clone();

    let writer_task = tokio::spawn(async move {
        write_blocks_to_chunks(rx, output_dir, completed_dir, compression_level, pb_clone, chunks_info).await
    });

    // Download all blocks across all chunks in parallel
    let all_blocks: Vec<_> = chunks_to_download
        .into_iter()
        .flat_map(|(chunk_id, blocks)| {
            blocks.into_iter().map(move |block_num| (chunk_id, block_num))
        })
        .collect();

    let mut failed = 0;
    {
        let mut stream = futures::stream::iter(all_blocks)
            .map(|(chunk_id, block_num)| {
                let client = client.clone();
                let bucket = args.bucket.clone();
                let request_payer = args.request_payer;
                let tx = tx.clone();
                async move {
                    let key = rmp_path(block_num);
                    match download_object(&client, &bucket, &key, request_payer).await {
                        Ok((key, data)) => {
                            let block = DownloadedBlock {
                                chunk_id,
                                block_num,
                                key,
                                data,
                            };
                            let _ = tx.send(block).await;
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
            })
            .buffer_unordered(args.concurrency);

        while let Some(result) = stream.next().await {
            if let Err(e) = result {
                failed += 1;
                pb.println(format!("Failed to download: {}", e));
            }
        }
    }

    // Close the channel and wait for writer to finish
    drop(tx);
    writer_task.await.context("Writer task failed")??;

    pb.finish_with_message("Download complete!");

    info!(
        "Successfully created checkpoints in {} ({} failed)",
        args.output_dir.display(),
        failed
    );

    // Merge all chunks into unified evm-blocks.tar.zst
    info!("Merging all chunks into unified evm-blocks.tar.zst...");
    let completed_dir = args.output_dir.join(".completed");
    merge_chunks_to_unified(&args.output_dir, &completed_dir, args.compression_level).await?;

    Ok(())
}

async fn write_blocks_to_chunks(
    mut rx: mpsc::Receiver<DownloadedBlock>,
    output_dir: PathBuf,
    completed_dir: PathBuf,
    compression_level: i32,
    pb: ProgressBar,
    chunks_info: BTreeMap<u64, Vec<u64>>,
) -> Result<()> {
    // Track blocks received per chunk
    let mut chunk_blocks: HashMap<u64, Vec<DownloadedBlock>> = HashMap::new();
    let expected_counts: HashMap<u64, usize> = chunks_info
        .iter()
        .map(|(id, blocks)| (*id, blocks.len()))
        .collect();

    // Track spawned write tasks
    let mut write_tasks = Vec::new();

    while let Some(block) = rx.recv().await {
        pb.inc(1);
        let chunk_id = block.chunk_id;

        chunk_blocks.entry(chunk_id).or_insert_with(Vec::new).push(block);

        // Check if this chunk is complete
        if let Some(expected) = expected_counts.get(&chunk_id) {
            if chunk_blocks.get(&chunk_id).unwrap().len() == *expected {
                // Chunk is complete, write it asynchronously
                let blocks = chunk_blocks.remove(&chunk_id).unwrap();
                let output_dir_clone = output_dir.clone();
                let completed_dir_clone = completed_dir.clone();
                let pb_clone = pb.clone();
                let expected_count = *expected;

                let write_task = tokio::task::spawn_blocking(move || {
                    write_chunk(chunk_id, blocks, &output_dir_clone, &completed_dir_clone, compression_level, &pb_clone, expected_count)
                });
                write_tasks.push(write_task);
            }
        }
    }

    // Wait for all write tasks to complete
    for task in write_tasks {
        task.await.context("Write task panicked")??;
    }

    Ok(())
}

fn write_chunk(
    chunk_id: u64,
    mut blocks: Vec<DownloadedBlock>,
    output_dir: &PathBuf,
    completed_dir: &PathBuf,
    compression_level: i32,
    pb: &ProgressBar,
    expected_count: usize,
) -> Result<()> {
    pb.println(format!("Writing chunk {} ({} blocks)...", chunk_id, blocks.len()));

    // Sort blocks by block number for consistent ordering
    blocks.sort_by_key(|b| b.block_num);

    let checkpoint_path = output_dir.join(format!("{}.tar.zst", chunk_id));

    // Create checkpoint file
    let output_file = std::fs::File::create(&checkpoint_path)
        .context(format!("Failed to create checkpoint file: {}", checkpoint_path.display()))?;

    let mut zstd_encoder = zstd::stream::write::Encoder::new(output_file, compression_level)
        .context("Failed to create zstd encoder")?;
    zstd_encoder.multithread(num_cpus::get() as u32)
        .context("Failed to enable multi-threaded compression")?;

    let mut tar_builder = Builder::new(zstd_encoder);

    // Add all blocks to tar
    for block in blocks {
        let mut header = tar::Header::new_gnu();
        header.set_size(block.data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();

        tar_builder.append_data(&mut header, &block.key, block.data.as_slice())
            .context(format!("Failed to add {} to tar", block.key))?;
    }

    // Finish the tar archive
    tar_builder.finish().context("Failed to finish tar archive")?;

    // Finish zstd compression
    let zstd_encoder = tar_builder.into_inner().context("Failed to get zstd encoder")?;
    zstd_encoder.finish().context("Failed to finish zstd compression")?;

    // Create appropriate marker based on block count
    if expected_count == 1000 {
        // Full chunk - create .completed marker
        let completed_marker = completed_dir.join(chunk_id.to_string());
        std::fs::write(&completed_marker, "")
            .context(format!("Failed to create completion marker: {}", completed_marker.display()))?;
        pb.println(format!("Chunk {} complete!", chunk_id));
    } else {
        // Partial chunk - create .partial marker
        let partial_marker = completed_dir.join(format!("{}.partial", chunk_id));
        std::fs::write(&partial_marker, "")
            .context(format!("Failed to create partial marker: {}", partial_marker.display()))?;
        pb.println(format!("Chunk {} complete ({} blocks, partial)!", chunk_id, expected_count));
    }

    Ok(())
}

async fn merge_chunks_to_unified(
    output_dir: &PathBuf,
    completed_dir: &PathBuf,
    compression_level: i32,
) -> Result<()> {
    // Find all chunk files (both .completed and .partial markers)
    let mut chunk_files = Vec::new();

    for entry in std::fs::read_dir(completed_dir).context("Failed to read .completed directory")? {
        let entry = entry?;
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();

        // Parse chunk_id from marker files
        let chunk_id = if filename_str.ends_with(".partial") {
            filename_str.trim_end_matches(".partial").parse::<u64>()
        } else {
            filename_str.parse::<u64>()
        };

        if let Ok(chunk_id) = chunk_id {
            let chunk_file = output_dir.join(format!("{}.tar.zst", chunk_id));
            if chunk_file.exists() {
                chunk_files.push((chunk_id, chunk_file));
            }
        }
    }

    // Sort chunk files by chunk_id
    chunk_files.sort_by_key(|(chunk_id, _)| *chunk_id);

    info!("Found {} chunk files to merge", chunk_files.len());

    if chunk_files.is_empty() {
        info!("No chunks to merge");
        return Ok(());
    }

    // Create unified tar.zst file first
    let unified_path = output_dir.join("evm-blocks.tar.zst");
    info!("Creating unified evm-blocks.tar.zst...");

    let output_file = std::fs::File::create(&unified_path)
        .context(format!("Failed to create unified file: {}", unified_path.display()))?;

    let mut zstd_encoder = zstd::stream::write::Encoder::new(output_file, compression_level)
        .context("Failed to create zstd encoder")?;
    zstd_encoder.multithread(num_cpus::get() as u32)
        .context("Failed to enable multi-threaded compression")?;

    let mut tar_builder = Builder::new(zstd_encoder);

    // Progress bar for chunks processed
    let pb = ProgressBar::new(chunk_files.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} Processing chunks...")
            .expect("Failed to set progress bar template")
            .progress_chars("#>-")
    );

    // Process chunks sequentially (they're already sorted by chunk_id)
    // This avoids loading all blocks into memory at once
    for (_chunk_id, chunk_file) in chunk_files {
        // Extract blocks from this chunk
        let blocks = tokio::task::spawn_blocking(move || {
            extract_blocks_from_chunk(&chunk_file)
        }).await.context("Extraction task failed")??;

        // Blocks within each chunk are already sorted by block_num (from write_chunk)
        // Write them directly to the unified archive
        for block in blocks {
            let mut header = tar::Header::new_gnu();
            header.set_size(block.data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();

            tar_builder.append_data(&mut header, &block.path, block.data.as_slice())
                .context(format!("Failed to add {} to unified tar", block.path))?;
        }

        pb.inc(1);
    }

    pb.finish_with_message("Processing complete!");

    // Finish the tar archive
    tar_builder.finish().context("Failed to finish unified tar archive")?;

    // Finish zstd compression
    let zstd_encoder = tar_builder.into_inner().context("Failed to get zstd encoder")?;
    zstd_encoder.finish().context("Failed to finish zstd compression")?;

    info!("Successfully created unified archive: {}", unified_path.display());

    Ok(())
}

fn extract_blocks_from_chunk(chunk_file: &PathBuf) -> Result<Vec<ExtractedBlock>> {
    let file = std::fs::File::open(chunk_file)
        .context(format!("Failed to open chunk file: {}", chunk_file.display()))?;

    let zstd_decoder = zstd::stream::read::Decoder::new(file)
        .context("Failed to create zstd decoder")?;

    let mut archive = Archive::new(zstd_decoder);
    let mut blocks = Vec::new();

    for entry in archive.entries().context("Failed to read archive entries")? {
        let mut entry = entry.context("Failed to read entry")?;

        let path = entry.path()
            .context("Failed to get entry path")?
            .to_string_lossy()
            .to_string();

        let mut data = Vec::new();
        entry.read_to_end(&mut data).context("Failed to read entry data")?;

        blocks.push(ExtractedBlock {
            path,
            data,
        });
    }

    Ok(blocks)
}

async fn download_object(
    client: &Client,
    bucket: &str,
    key: &str,
    request_payer: bool,
) -> Result<(String, Vec<u8>)> {
    let mut request = client
        .get_object()
        .bucket(bucket)
        .key(key);

    if request_payer {
        request = request.request_payer(aws_sdk_s3::types::RequestPayer::Requester);
    }

    let response = request.send().await
        .context(format!("Failed to download object: {}", key))?;

    let data = response.body.collect().await
        .context("Failed to collect object data")?
        .into_bytes()
        .to_vec();

    Ok((key.to_string(), data))
}
