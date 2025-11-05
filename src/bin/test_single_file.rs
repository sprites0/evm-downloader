use std::io::Read;
use tar::Archive;
use anyhow::Result;

fn main() -> Result<()> {
    let file = std::fs::File::open("blocks/357000.tar.zst")?;
    let zstd_decoder = zstd::stream::read::Decoder::new(file)?;
    let mut archive = Archive::new(zstd_decoder);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_string_lossy().to_string();

        if path.contains("357102") {
            println!("Found: {}", path);
            println!("Entry size: {:?}", entry.size());

            let mut data = Vec::new();
            match entry.read_to_end(&mut data) {
                Ok(n) => println!("Read {} bytes successfully", n),
                Err(e) => println!("Error reading: {:?}", e),
            }

            println!("Data length: {}", data.len());

            // Try to decompress
            let mut decoder = lz4_flex::frame::FrameDecoder::new(&data[..]);
            let mut decompressed = Vec::new();
            match decoder.read_to_end(&mut decompressed) {
                Ok(n) => println!("Decompressed {} bytes successfully", n),
                Err(e) => println!("Error decompressing: {:?}", e),
            }
        }
    }

    Ok(())
}
