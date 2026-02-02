use polars::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;

#[derive(Debug, PartialEq, Eq, Hash)]
struct VideoGroup {
    name: String,
    fps: u32,
}

fn main() -> Result<(), Box<dyn Error>> {
    let codec_prefix = "HEVC";
    // Regex to capture: 1: Name, 2: FPS, 3: Mbps
    let re = Regex::new(&format!(
        r"{codec_prefix}_(.*)_(\d+)fps_(\d+)Mbps_framesizes\.csv",
    ))?;

    // Map to group files: Key -> Vec<(Mbps, Path)>c
    let mut groups: HashMap<VideoGroup, Vec<(u32, String)>> = HashMap::new();

    // 1. Scan directory and group files
    for entry in std::fs::read_dir("/home/boris/Desktop/merge_hevc/")? {
        let path = entry?.path();
        let filename = path.file_name().unwrap().to_string_lossy();

        if let Some(cap) = re.captures(&filename) {
            let video_name = cap[1].to_string();
            let fps = cap[2].parse::<u32>()?;
            let mbps = cap[3].parse::<u32>()?;

            let group = VideoGroup {
                name: video_name,
                fps,
            };
            groups
                .entry(group)
                .or_default()
                .push((mbps, path.to_string_lossy().into_owned()));
        }
    }

    // 2. Process each group into its own CSV
    for (group, mut files) in groups {
        println!("Processing {} at {}fps...", group.name, group.fps);

        // Sort files by bitrate (Mbps)
        files.sort_by_key(|f| f.0);

        // Initialize DataFrame with the first file in the sorted group
        // ... inside your loop
        let (first_mbps, first_path) = &files[0];

        // FIX: Open file first, then pass to CsvReader::new()
        let file = File::open(first_path)?;
        let mut combined_df = CsvReader::new(file).finish()?;
        combined_df.rename("bytes", format!("{}Mbps", first_mbps).into())?;

        // Join subsequent files
        for (mbps, path) in files.iter().skip(1) {
            // FIX: Same here
            let next_file = File::open(path)?;
            let next_df = CsvReader::new(next_file)
                .finish()?
                .select(["frame_index", "bytes"])?;

            // Note: rename is usually done on the DataFrame after finish()
            let mut next_df = next_df;
            next_df.rename("bytes", format!("{}Mbps", mbps).into())?;

            combined_df = combined_df.left_join(&next_df, ["frame_index"], ["frame_index"])?;
        }

        // 3. Save the specific group file
        let output_name = format!("{}_{}_{}fps.csv", codec_prefix, group.name, group.fps);
        let mut out_file = File::create(&output_name)?;
        CsvWriter::new(&mut out_file).finish(&mut combined_df)?;

        println!("Saved to {}", output_name);
    }

    Ok(())
}
