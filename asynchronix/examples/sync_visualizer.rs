use anyhow::Result;
use minifb::{Key, Window, WindowOptions};
use std::fs::File;
use std::io::Read;
use std::time::{Duration, Instant};

// Define constants
const WIDTH: usize = 1920;
const HEIGHT: usize = 1080;
const WINDOW_SCALE_FACTOR: f64 = 0.6; // Keep small to fit side-by-side on screen
const DEFAULT_PLAYBACK_FPS: u32 = 15; // Default frames per second when playing

/// Loads an RGB frame from a file and converts to minifb's u32 format
fn load_rgb_frame(path: &str) -> Result<Vec<u32>> {
    let mut file = File::open(path)?;
    let mut rgb_data = vec![0u8; WIDTH * HEIGHT * 3];
    file.read_exact(&mut rgb_data)?;

    // Convert RGB to u32 (0xRRGGBB)
    let mut pixels = Vec::with_capacity(WIDTH * HEIGHT);
    for chunk in rgb_data.chunks_exact(3) {
        let pixel = ((chunk[0] as u32) << 16) | ((chunk[1] as u32) << 8) | (chunk[2] as u32);
        pixels.push(pixel);
    }

    Ok(pixels)
}

/// Scale a pixel buffer to a new size
fn scale_buffer(
    buffer: &[u32],
    width: usize,
    height: usize,
    new_width: usize,
    new_height: usize,
) -> Vec<u32> {
    let mut scaled = vec![0u32; new_width * new_height];
    let x_ratio = (width << 16) / new_width;
    let y_ratio = (height << 16) / new_height;

    for y in 0..new_height {
        let y2 = ((y * y_ratio) >> 16) * width;
        for x in 0..new_width {
            let x2 = (x * x_ratio) >> 16;
            scaled[y * new_width + x] = buffer[y2 + x2];
        }
    }
    scaled
}

/// Main function to visualize frame pairs side-by-side
fn main() -> Result<()> {
    println!("Frame Sync Visualizer with Fine-Grained Controls");

    // Parse the CSV file with frame pairs
    let csv_content = std::fs::read_to_string("Video_Sink/synced_frames.csv")?;
    let mut frame_pairs = Vec::new();

    for line in csv_content.lines().skip(1) {
        // Skip header
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 4 {
            let frame_number: u64 = parts[0].parse()?;
            let ref_path = parts[2].to_string();
            let lossy_path = parts[3].to_string();

            // Check if both files exist
            if std::path::Path::new(&ref_path).exists()
                && std::path::Path::new(&lossy_path).exists()
            {
                frame_pairs.push((frame_number, ref_path, lossy_path));
            }
        }
    }

    if frame_pairs.is_empty() {
        return Err(anyhow::anyhow!("No valid frame pairs found"));
    }

    println!("Found {} frame pairs", frame_pairs.len());

    // Set up the window for side-by-side display
    let scaled_width = (WIDTH as f64 * WINDOW_SCALE_FACTOR) as usize;
    let scaled_height = (HEIGHT as f64 * WINDOW_SCALE_FACTOR) as usize;
    let window_width = scaled_width * 2 + 10; // Add some gap between frames

    let mut window = Window::new(
        "Frame Sync Visualizer",
        window_width,
        scaled_height,
        WindowOptions::default(),
    )?;

    // Create a buffer for the combined display
    let mut buffer = vec![0u32; window_width * scaled_height];

    // Index of current frame pair
    let mut current_idx = 0;

    // Playback control variables
    let mut is_playing = true; // Start in playing mode
    let mut playback_speed = DEFAULT_PLAYBACK_FPS;
    let frame_duration = Duration::from_secs_f64(1.0 / playback_speed as f64);
    let mut next_frame_time = Instant::now();

    // Add text overlay to show keyboard controls
    println!("Controls:");
    println!("  Space: Toggle play/pause");
    println!("  Left/Right: Previous/Next frame (when paused)");
    println!("  Up/Down: Increase/Decrease playback speed");
    println!("  Esc: Exit");

    while window.is_open() && !window.is_key_down(Key::Escape) {
        let now = Instant::now();

        // Handle keyboard input
        if window.is_key_pressed(Key::Space, minifb::KeyRepeat::No) {
            is_playing = !is_playing;
            if is_playing {
                next_frame_time = Instant::now(); // Reset timer when resuming play
            }
            println!("{}", if is_playing { "Playing" } else { "Paused" });
        }

        // Playback speed control
        if window.is_key_pressed(Key::Up, minifb::KeyRepeat::Yes) {
            playback_speed = (playback_speed + 1).min(60);
            println!("Playback speed: {} FPS", playback_speed);
        }

        if window.is_key_pressed(Key::Down, minifb::KeyRepeat::Yes) {
            playback_speed = (playback_speed - 1).max(1);
            println!("Playback speed: {} FPS", playback_speed);
        }

        // Frame navigation
        let mut should_advance = false;

        if is_playing {
            // In playing mode, advance frames based on playback speed
            if now >= next_frame_time {
                should_advance = true;
                next_frame_time = now + Duration::from_secs_f64(1.0 / playback_speed as f64);
            }
        } else {
            // In paused mode, manual frame navigation
            if window.is_key_pressed(Key::Right, minifb::KeyRepeat::Yes) {
                current_idx = (current_idx + 1) % frame_pairs.len();
                should_advance = true; // Force refresh
            }

            if window.is_key_pressed(Key::Left, minifb::KeyRepeat::Yes) {
                current_idx = if current_idx == 0 {
                    frame_pairs.len() - 1
                } else {
                    current_idx - 1
                };
                should_advance = true; // Force refresh
            }
        }

        // Advance to next frame in playing mode
        if is_playing && should_advance {
            current_idx = (current_idx + 1) % frame_pairs.len();
        }

        // Get current frame pair and update display
        let (frame_number, ref_path, lossy_path) = &frame_pairs[current_idx];

        // Load and scale the reference frame
        match load_rgb_frame(ref_path) {
            Ok(pixels) => {
                let scaled = scale_buffer(&pixels, WIDTH, HEIGHT, scaled_width, scaled_height);

                // Copy the scaled reference frame to the left side of the buffer
                for y in 0..scaled_height {
                    for x in 0..scaled_width {
                        buffer[y * window_width + x] = scaled[y * scaled_width + x];
                    }
                }
            }
            Err(e) => {
                eprintln!("Error loading reference frame: {}", e);
                // Fill with red if there's an error
                for y in 0..scaled_height {
                    for x in 0..scaled_width {
                        buffer[y * window_width + x] = 0xFF0000;
                    }
                }
            }
        }

        // Add a vertical separator line
        for y in 0..scaled_height {
            for x in 0..10 {
                buffer[y * window_width + scaled_width + x] = 0x808080;
            }
        }

        // Load and scale the lossy frame
        match load_rgb_frame(lossy_path) {
            Ok(pixels) => {
                let scaled = scale_buffer(&pixels, WIDTH, HEIGHT, scaled_width, scaled_height);

                // Copy the scaled lossy frame to the right side of the buffer
                for y in 0..scaled_height {
                    for x in 0..scaled_width {
                        buffer[y * window_width + scaled_width + 10 + x] =
                            scaled[y * scaled_width + x];
                    }
                }
            }
            Err(e) => {
                eprintln!("Error loading lossy frame: {}", e);
                // Fill with blue if there's an error
                for y in 0..scaled_height {
                    for x in 0..scaled_width {
                        buffer[y * window_width + scaled_width + 10 + x] = 0x0000FF;
                    }
                }
            }
        }

        // Update window with the buffer
        window.update_with_buffer(&buffer, window_width, scaled_height)?;

        // Update window title with frame info and playback status
        let status = if is_playing {
            format!("PLAYING ({} FPS)", playback_speed)
        } else {
            "PAUSED".to_string()
        };

        window.set_title(&format!(
            "Frame {}: {}/{} - {} - Space: Play/Pause, Arrows: Navigate",
            frame_number,
            current_idx + 1,
            frame_pairs.len(),
            status
        ));

        // Throttle update rate to reduce CPU usage when paused
        if !is_playing {
            std::thread::sleep(Duration::from_millis(50));
        } else {
            // When playing, just yield briefly to not hog the CPU
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    println!("Visualizer closed");
    Ok(())
}
