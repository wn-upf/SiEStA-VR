use anyhow::Result;
use std::io::{BufReader, Read};
use ffmpeg_sidecar::command::FfmpegCommand;


// Define the expected (encoder) dimensions.
pub const WIDTH_ENCODER: usize = 1920;
pub const HEIGHT_ENCODER: usize = 1080;

/// Converts raw RGB byte data (3 bytes per pixel) into a Vec<u32> pixel buffer
/// where each pixel is represented as 0xRRGGBB.
fn convert_rgb_to_u32(rgb_data: &[u8], width: usize, height: usize) -> Vec<u32> {
    if rgb_data.len() != width * height * 3 {
        eprintln!(
            "Unexpected RGB data length. Expected {}, got {}",
            width * height * 3,
            rgb_data.len()
        );
        return Vec::new();
    }
    rgb_data
        .chunks_exact(3)
        .map(|chunk| {
            let r = chunk[0] as u32;
            let g = chunk[1] as u32;
            let b = chunk[2] as u32;
            (r << 16) | (g << 8) | b
        })
        .collect()
}



/// A HEVC decoder that spawns an ffmpeg process (via ffmpeg-sidecar)
/// which reads an MP4-encoded HEVC stream from stdin, decodes it using
/// the hevc_nvdec hardware decoder (with CUDA), applies a stable framerate
/// filter, converts to RGB24, and outputs raw video frames (rawvideo) to stdout.
pub struct HevcDecoder {
    reader: BufReader<std::process::ChildStdout>,
    frame_size: usize,
    width: u32,
    height: u32,
}

impl HevcDecoder {
    /// Creates a new HevcDecoder.
    ///
    /// - `framerate`: The desired stable output frame rate (e.g., 30).
    /// - `width` and `height`: The expected output frame dimensions.
    ///
    /// This function spawns an ffmpeg process with a command roughly equivalent to:
    ///
    /// ```bash
    /// ffmpeg -hwaccel cuda -f mp4 -i - \
    ///   -c:v hevc_nvdec \
    ///   -vf "fps=30" \
    ///   -pix_fmt rgb24 \
    ///   -f rawvideo -
    /// ```
    ///
    /// That is, it reads an MP4 stream from stdin, decodes using HEVC NVDEC with CUDA,
    /// forces 30 fps output, converts frames to rgb24, and writes raw video to stdout.
    pub fn new(framerate: u32, width: u32, height: u32) -> Result<Self> {
        // For raw RGB24, each frame is width * height * 3 bytes.
        let frame_size = (width as usize) * (height as usize) * 3;
        
        // Build the ffmpeg command.
        // We set the input format to mp4, use "-" to indicate reading from stdin.
        let mut ffmpeg_cmd_builder = FfmpegCommand::new();
        let ffmpeg_cmd = ffmpeg_cmd_builder
            .args(&["-hwaccel", "cuda"])
            .args(&["-f", "mp4", "-i", "-"]) // input from stdin (an MP4 container)
            .args(&["-c:v", "hevc_nvdec"])     // use NVIDIA hardware decoder for HEVC
            .args(&["-vf", &format!("fps={}", framerate)]) // force a stable framerate
            .args(&["-pix_fmt", "rgb24"])      // output pixel format: RGB24
            .args(&["-f", "rawvideo", "-"]);   // output raw video to stdout

        // Spawn the process and capture its stdout.
        let mut child = ffmpeg_cmd.spawn()?;
        let stdout = child
            .take_stdout()
            .expect("Failed to capture ffmpeg stdout");
        let reader = BufReader::new(stdout);

        Ok(HevcDecoder {
            reader,
            frame_size,
            width,
            height,
        })
    }

    /// Reads the next decoded frame from ffmpeg’s stdout.
    ///
    /// This function attempts to read exactly one frame (frame_size bytes)
    /// from the process’s stdout. If fewer bytes are available (EOF), it returns None.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; self.frame_size];
        match self.reader.read_exact(&mut buf) {
            Ok(()) => Ok(Some(buf)),
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}



/// Spawns an ffmpeg process (via ffmpeg-sidecar) that reads an input file,
/// decodes it to raw RGB frames, and returns one frame as a u32 pixel buffer.
/// The underlying ffmpeg command is equivalent to:
///
/// ```bash
/// ffmpeg -hwaccel cuda -i input.mp4 -pix_fmt rgb24 \
///   -vf "scale=WIDTH:HEIGHT,format=rgb24" \
///   -c:v rawvideo -f rawvideo -
/// ```
fn get_decoded_frame() -> Vec<u32> {
    let input_path = "input.mp4"; // Replace with your actual input video path.
    // Build the ffmpeg command.
    let mut ffmpeg_cmd_builder = FfmpegCommand::new(); 
    let mut ffmpeg_cmd = ffmpeg_cmd_builder
        .input(input_path)
        .args(&["-hwaccel", "cuda"])
        .args(&["-pix_fmt", "rgb24"])
        .args(&[
            "-vf",
            &format!("scale={}:{}{},format=rgb24", WIDTH_ENCODER, HEIGHT_ENCODER, ""),
        ])
        .args(&["-c:v", "rawvideo"])
        .args(&["-f", "rawvideo", "-"]); // Write raw video to stdout.

    // Spawn the ffmpeg process.
    let mut child = ffmpeg_cmd.spawn().expect("failed to spawn ffmpeg");
    let stdout = child
        .take_stdout()
        .expect("Failed to capture ffmpeg stdout");
    let mut reader = BufReader::new(stdout);

    // Calculate expected frame size (RGB24: 3 bytes per pixel)
    let frame_size = WIDTH_ENCODER * HEIGHT_ENCODER * 3;
    let mut frame_buf = vec![0u8; frame_size];

    match reader.read_exact(&mut frame_buf) {
        Ok(()) => {
            println!("Decoded frame data length: {} Kb", frame_size / 1000);
            convert_rgb_to_u32(&frame_buf, WIDTH_ENCODER, HEIGHT_ENCODER)
        }
        Err(e) => {
            eprintln!("Error reading FFmpeg output: {}", e);
            Vec::new()
        }
    }
}
/// Scales the pixel buffer (using nearest-neighbor scaling) from (orig_width x orig_height)
/// to (new_width x new_height).
fn scale_pixels(
    buffer: &[u32],
    orig_width: usize,
    orig_height: usize,
    new_width: usize,
    new_height: usize,
) -> Vec<u32> {
    let mut scaled = vec![0u32; new_width * new_height];
    for y in 0..new_height {
        let orig_y = y * orig_height / new_height;
        for x in 0..new_width {
            let orig_x = x * orig_width / new_width;
            scaled[y * new_width + x] = buffer[orig_y * orig_width + orig_x];
        }
    }
    scaled
}

/// A HEVC encoder that spawns an ffmpeg process (via ffmpeg-sidecar)
/// which encodes the input video using hevc_nvenc with CBR low-latency
/// settings and a GOP size of 90.
pub struct HevcEncoder {
    reader: BufReader<std::process::ChildStdout>,
}

impl HevcEncoder {
    /// Create a new HevcEncoder.
    ///
    /// - `input`: Path to the input video.
    /// - `width` and `height`: The dimensions to scale the video.
    /// - `bitrate`: The target bitrate (e.g., "1000k").
    pub fn new(input: &str, width: u32, height: u32, bitrate: &str) -> Result<Self> {
        // Build the ffmpeg command.
        // This command uses CUDA, scales the video,
        // encodes with hevc_nvenc in CBR mode with low latency,
        // forces a GOP size of 90, and outputs a fragmented MP4 stream.
        
        let mut ffmpeg_cmd_builder = FfmpegCommand::new(); 
        
        let ffmpeg_cmd = ffmpeg_cmd_builder
            .input(input)
            .args(&["-hwaccel", "cuda"])
            .args(&[
                "-vf",
                &format!("scale={}:{}{},format=yuv420p", width, height, ""),
            ])
            .args(&["-c:v", "hevc_nvenc"])
            .args(&["-preset", "llhp"])
            .args(&["-rc", "cbr"])
            .args(&["-b:v", bitrate, "-maxrate", bitrate, "-bufsize", bitrate])
            .args(&["-rc-lookahead", "0"])
            .args(&["-g", "90"])
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            .args(&["-an"])
            .args(&["-f", "mp4", "-"]); // output to stdout

        // Spawn the process and capture its stdout.
        let mut child = ffmpeg_cmd.spawn()?;
        let stdout = child.take_stdout().expect("Failed to capture stdout");
        let reader = BufReader::new(stdout);

        Ok(HevcEncoder { reader })
    }

    /// Reads the next chunk of encoded data (packet) from stdout.
    /// (This simplistic implementation reads up to 4096 bytes at a time.)
    pub fn next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; 4096];
        let bytes_read = self.reader.read(&mut buf)?;
        if bytes_read == 0 {
            Ok(None)
        } else {
            buf.truncate(bytes_read);
            Ok(Some(buf))
        }
    }
}

fn main() -> Result<()> {
    // Ensure that ffmpeg is available (it downloads automatically if needed).
    ffmpeg_sidecar::download::auto_download()?;
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_sunflower_2160p_60fps_stereo_abl.mp4";
    pub const WIDTH_ENCODER: usize = 1920;
    pub const HEIGHT_ENCODER: usize = 1080;
    // Create a HevcEncoder that encodes "input.mp4" to 1280x720 with 1000k bitrate.
    let mut encoder = HevcEncoder::new(&input_path, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, "1000k")?;

    // In a loop, read and process encoded packets.
    while let Some(packet) = encoder.next_packet()? {
        println!("Got packet of {} bytes", packet.len());
        // Here you can, for example, push the packet into an async channel,
        // or process it further for low-latency streaming.
    }

    Ok(())
}
