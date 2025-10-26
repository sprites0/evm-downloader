# Super Fast S3 Downloader

High-performance AWS S3 downloader for EVM block data with tar.zst compression.

## Build

```bash
cargo build --release
```

## Usage

```bash
# Download from start block to latest (fetched from RPC)
super-fast-s3 --start-block 1000000

# Download specific range
super-fast-s3 --start-block 1000000 --end-block 2000000
```

## Key Options

- `--start-block <NUM>` - Starting block (required)
- `--end-block <NUM>` - Ending block (optional, defaults to RPC latest)
- `-o, --output-dir <DIR>` - Output directory (default: "blocks")
- `-b, --bucket <BUCKET>` - S3 bucket (default: "hl-mainnet-evm-blocks")
- `-r, --region <REGION>` - AWS region (default: "ap-northeast-1")
- `-c, --compression-level <LEVEL>` - Compression 1-22 (default: 3)
- `--concurrency <NUM>` - Concurrent downloads (default: 100)

## AWS Credentials

Set credentials via environment variables, `~/.aws/credentials`, or IAM role:
```bash
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
```

## Resume Downloads

Run the same command again to automatically skip completed chunks.

## Extract Archives

```bash
tar --use-compress-program=unzstd -xvf blocks/evm-blocks.tar.zst
```
