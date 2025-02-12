use anyhow::Result;
use std::io::{BufReader, Read, Write};
use std::process::{ChildStdin, ChildStdout};
use ffmpeg_sidecar::command::{FfmpegCommand};
use minifb::{Key, Scale, Window, WindowOptions};
use rand::Rng;


// Define the expected (encoder) dimensions.
pub const WIDTH_ENCODER: usize = 3840;
pub const HEIGHT_ENCODER: usize = 4320;

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
/// which encodes the input video using hevc_nvenc in CBR low‑latency mode
/// (with a GOP of 90) and outputs a fragmented MP4 stream to stdout.
pub struct HevcEncoder {
    reader: BufReader<ChildStdout>,
    _child: ffmpeg_sidecar::child::FfmpegChild, // Keep child process alive
    stderr: std::process::ChildStderr, 
}

impl HevcEncoder {
    pub fn new(input: &str, width: u32, height: u32, bitrate: &str) -> Result<Self> {
        // First, verify the input file exists
        if !std::path::Path::new(input).exists() {
            return Err(anyhow::anyhow!("Input file does not exist: {}", input));
        }
    
        // Build the ffmpeg command with logging.
        let mut ffmpeg_cmd_builder = FfmpegCommand::new();
        let ffmpeg_cmd = ffmpeg_cmd_builder
            .args(&["-stream_loop", "-1"]) // Loop input indefinitely
            // .args(&["-loglevel", "quiet"])  // Suppress informational output
            .input(input)
            .args(&[
                "-vf",
                &format!("scale={}:{}:force_original_aspect_ratio=disable,format=yuv420p", width, height),
            ])
            .args(&["-c:v", "hevc_nvenc"])
            .args(&["-preset", "p6"])  // using a preset available on your NVENC version
            .args(&["-rc", "cbr"])
            .args(&["-b:v", bitrate, "-maxrate", bitrate, "-bufsize", "2000k"]) 
            .args(&["-rc-lookahead", "0"])
            .args(&["-threads", "2"])  // Allow parallel processing
            .args(&["-g", "90"])
            .args(&["-movflags", "+frag_keyframe+empty_moov"])
            // In HevcEncoder's FFmpeg command:
            .args(&["-flush_packets", "1"])  // Immediately emit packets
            .args(&["-frag_duration", "1000"])  // 1ms fragments for low latency
            .args(&["-an"])
            .args(&["-f", "mp4", "-"]); // output to stdout
    
        // Spawn the process and capture both stdout and stderr.
        let mut child: ffmpeg_sidecar::child::FfmpegChild = ffmpeg_cmd.spawn()?;
        
        let stdout = child.take_stdout().expect("Failed to capture stdout");
        let mut stderr = child.take_stderr().expect("Failed to capture stderr"); 
    
        // (Optionally) Wait a short time and check stderr.
        std::thread::sleep(std::time::Duration::from_millis(100));
        let mut err_buf = [0u8; 90000];
        // If any error message appears (beyond banner info) you could inspect here.
        if let Ok(n) = stderr.read(&mut err_buf) {
            if n > 0 {
                // You can choose to log these messages without failing.
                eprintln!("FFmpeg stderr: {}", String::from_utf8_lossy(&err_buf[..n]));
                // Or, if you want to treat actual errors differently, you could parse the output.
            }
        }
    
        let reader = BufReader::new(stdout);
    
        Ok(HevcEncoder { 
            reader,
            _child: child, // Keep child process alive
            stderr, 
        })
    }
    

    pub fn next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; 4096];
        match self.reader.read(&mut buf) {
            Ok(0) => Ok(None),
            Ok(n) => {
                buf.truncate(n);
                Ok(Some(buf))
            }
            Err(e) => Err(e.into())
        }
    }

     // Add a method to read stderr
     pub fn check_errors(&mut self) -> Result<()> {
        let stderr = &mut self.stderr;
        
        let mut buf = String::new();
        stderr.read_to_string(&mut buf)?;
        if !buf.is_empty() {
            eprintln!("FFmpeg Encoder Errors:\n{}", buf);
            }
        // }
        Ok(())
    }
}

/// A HEVC decoder that spawns an ffmpeg process (via ffmpeg-sidecar)
/// which reads an MP4-encoded HEVC stream from its stdin,
/// decodes it using hevc_nvdec (with CUDA) at a stable framerate,
/// converts frames to RGB24, and outputs raw video frames to stdout.
///
/// (We modified this struct to capture both stdin (for feeding encoded data)
/// and stdout (for reading decoded raw frames).)
pub struct HevcDecoder {
    reader: BufReader<ChildStdout>,
    writer: std::process::ChildStdin,
    frame_size: usize,
    width: u32,
    height: u32,
    stderr: std::process::ChildStderr , 
    buffer: Vec<u8>, 
}

impl HevcDecoder {
    /// Creates a new HevcDecoder.
    ///
    /// - `framerate`: Desired output framerate (e.g., 30).
    /// - `width` and `height`: Expected output frame dimensions.
    ///
    /// The underlying ffmpeg command is roughly equivalent to:
    ///
    /// ```bash
    /// ffmpeg -hwaccel cuda -f mp4 -i - \
    ///   -c:v hevc_nvdec \
    ///   -vf "fps=30" \
    ///   -pix_fmt rgb24 \
    ///   -f rawvideo -
    /// ```
    pub fn new(framerate: u32, width: u32, height: u32) -> Result<Self> {
        let frame_size = (width as usize) * (height as usize) * 3;
        let mut ffmpeg_cmd_builder = FfmpegCommand::new();
        let ffmpeg_cmd = ffmpeg_cmd_builder
            // .args(&["-hwaccel", "cuda"])
            .args(&["-f", "mp4", "-i", "-"]) // input from stdin
            .args(&["-c:v", "hevc_nvdec"])
            .args(&["-vf", &format!("fps={}", framerate)])
            .args(&["-pix_fmt", "rgb24"])
            .args(&["-f", "rawvideo", "-"]); // output raw video to stdout

        let mut child = ffmpeg_cmd.spawn()?;
        
        let stdout = child
            .take_stdout()
            .expect("Failed to capture ffmpeg decoder stdout");
        let stdin = child
            .take_stdin()
            .expect("Failed to capture ffmpeg decoder stdin");
        let stderr = child.take_stderr().expect("FAILED TO CAPTURE STDERR"); 
        let reader = BufReader::new(stdout);

        Ok(HevcDecoder {
            reader,
            writer: stdin,
            frame_size,
            width,
            height,
            stderr, 
            buffer: Vec::with_capacity(frame_size * 2),  // Add this
        })
    }

    /// Feeds an encoded packet into the decoder's stdin.
    pub fn feed_packet(&mut self, packet: &[u8]) -> Result<()> {
        self.writer.write_all(packet)?;
        Ok(())
    }
    pub fn check_errors(&mut self) -> Result<()> {
        let mut buf = [0u8; 4096];
        if let Ok(n) = self.stderr.read(&mut buf) {
            if n > 0 {
                eprintln!("DECODER ERROR: {}", String::from_utf8_lossy(&buf[..n]));
            }
        }
        Ok(())
    }
    /// Reads the next decoded frame from stdout.
    /// Reads exactly one frame (frame_size bytes) or returns None if EOF.
    pub fn next_frame(&mut self) -> Result<Option<Vec<u8>>> {
        // Read until we have at least one full frame
        while self.buffer.len() < self.frame_size {
            let mut chunk = vec![0u8; 4096];
            let n = self.reader.read(&mut chunk)?;
            if n == 0 {
                return if self.buffer.is_empty() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("Partial frame at EOF"))
                };
            }
            self.buffer.extend_from_slice(&chunk[..n]);
        }

        // Extract one frame
        let frame = self.buffer.drain(..self.frame_size).collect();
        Ok(Some(frame))
    }
}

fn main() -> Result<()> {
    // Ensure ffmpeg is available.
    ffmpeg_sidecar::download::auto_download()?;

    // Input video path.
    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_sunflower_2160p_60fps_stereo_abl.mp4";

    // Create an encoder that produces encoded MP4 packets.
    let mut encoder = HevcEncoder::new(input_path, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32, "1000k")?;
    
    // Create a decoder that will output raw RGB frames at 30 fps.
    let mut decoder = HevcDecoder::new(60, WIDTH_ENCODER as u32, HEIGHT_ENCODER as u32)?;

    // Set the probability of dropping a frame (in [0,1]). For example, 30% drop.
    let drop_probability: f64 = 0.0;
    let mut rng = rand::thread_rng();

    // Set up the display window.
    let scale_factor = 0.4;
    let scaled_width = (WIDTH_ENCODER as f64 * scale_factor) as usize;
    let scaled_height = (HEIGHT_ENCODER as f64 * scale_factor) as usize;
    let mut window = Window::new(
        "Decoded HEVC with Dropped Frames",
        scaled_width,
        scaled_height,
        WindowOptions {
            scale: Scale::X1, // We scale manually.
            ..WindowOptions::default()
        },
    )
    .expect("Unable to create window");

    // Main loop: read encoded packets from the encoder.
    // With probability P, drop the packet; otherwise, feed it to the decoder.
    // Then, try to read a decoded frame and display it.
    loop {
        // Attempt to get an encoded packet from the encoder.
        match encoder.next_packet()? {
            Some(packet) => {
                // Uniformly sample a random number in [0,1]
                let sample: f64 = rng.gen();
                if sample < drop_probability {
                    println!("Dropped an encoded packet (simulated loss).");
                    // We simulate a drop by not feeding the packet.
                } else {
                    // Feed the packet to the decoder.
                    decoder.feed_packet(&packet)?;
                }

                // Try to read a decoded frame from the decoder.
                if let Some(frame_bytes) = decoder.next_frame()? {
                    // Convert raw RGB (3 bytes per pixel) to u32 pixels.
                    let pixels = convert_rgb_to_u32(&frame_bytes, WIDTH_ENCODER, HEIGHT_ENCODER);
                    if pixels.is_empty() {
                        eprintln!("No pixels decoded.");
                        continue;
                    }
                    // Scale the pixel buffer for display.
                    let scaled_pixels = scale_pixels(&pixels, WIDTH_ENCODER, HEIGHT_ENCODER, scaled_width, scaled_height);
                    window.update_with_buffer(&scaled_pixels, scaled_width, scaled_height)
                        .expect("Failed to update window");
                }
            }
            None => {
                println!("No more encoded packets from encoder.");
                let err = encoder.check_errors(); 
                println!("{:?}", err); 
                continue;
            }
        }

        // Allow the window to process events.
        if !window.is_open() || window.is_key_down(Key::Escape) {
            break;
        }
    }

    Ok(())
}
