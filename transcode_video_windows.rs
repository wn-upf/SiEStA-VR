extern crate ffmpeg_next as ffmpeg;

use ffmpeg::codec::{self, context::Context};
use ffmpeg::format::{input, output, Pixel};
use ffmpeg::media::Type;
use ffmpeg::software::scaling::{context::Context as ScaleContext, flag::Flags};
use ffmpeg::util::frame::video::Video;
use std::env;
use std::fs::{File, create_dir_all};
use std::io::prelude::*;
use std::path::PathBuf;


use ffmpeg_next::Packet; 

fn main() -> Result<(), ffmpeg::Error> {
    ffmpeg::init()?;

    let input_path = "/home/boris/Desktop/Rust_MG1/asynchronix/BigBuckBunny.mp4";
    let output_dir = "/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink";
    
    // Make sure the output directory exists
    create_dir_all(output_dir)?;

    // Open the input video file
    let mut ictx = input(&PathBuf::from(input_path))?;
    let input = ictx
        .streams()
        .best(Type::Video)
        .ok_or(ffmpeg::Error::StreamNotFound)?;
    let video_stream_index = input.index();

    let context_decoder = ffmpeg::codec::context::Context::from_parameters(input.parameters())?;
    let mut decoder = context_decoder.decoder().video()?;

    let mut scaler = ScaleContext::get(
        decoder.format(),
        decoder.width(),
        decoder.height(),
        Pixel::RGB24,
        decoder.width(),
        decoder.height(),
        Flags::BILINEAR,
    )?;

    let mut frame_index = 0;
    let mut second_counter = 0;

    let mut receive_and_process_decoded_frames = |decoder: &mut ffmpeg::decoder::Video| -> Result<(), ffmpeg::Error> {
        let mut decoded = Video::empty();
        while decoder.receive_frame(&mut decoded).is_ok() {
            let mut rgb_frame = Video::empty();
            scaler.run(&decoded, &mut rgb_frame)?;

            // Only process one frame per second
            if second_counter % 30 == 0 { // Assuming 30 fps, change based on your video's fps
                let encoded_frame = encode_to_hevc(&rgb_frame)?;
                save_encoded_frame(&encoded_frame, frame_index)?;
                frame_index += 1;
            }
        }
        Ok(())
    };

    // Process packets from the video stream
    for (stream, packet) in ictx.packets() {
        if stream.index() == video_stream_index {
            decoder.send_packet(&packet)?;
            receive_and_process_decoded_frames(&mut decoder)?;
            second_counter += 1;
        }
    }
    decoder.send_eof()?;
    receive_and_process_decoded_frames(&mut decoder)?;

    Ok(())
}

// Encode a frame to HEVC
fn encode_to_hevc(frame: &Video) -> Result<Vec<u8>, ffmpeg::Error> {
    // Create the HEVC encoder context
    let encoder = codec::encoder::find(codec::Id::HEVC)
        .ok_or(ffmpeg::Error::EncoderNotFound)?
        .video()?;

    let mut encoder_ctx = Context::new(codec::Id::HEVC)?;
    encoder_ctx.set_width(frame.width());
    encoder_ctx.set_height(frame.height());
    encoder_ctx.set_format(Pixel::YUV420P); // Common format for HEVC encoding

    encoder_ctx.open_as(&encoder)?;

    let mut encoded_data = Vec::new();
    let mut packet = ffmpeg::util::packet::Packet::empty();

    // Send frame for encoding
    encoder_ctx.send_frame(frame)?;

    while encoder_ctx.receive_packet(&mut packet).is_ok() {
        encoded_data.extend_from_slice(packet.data());
    }

    Ok(encoded_data)
}

// Save the encoded HEVC frame to a file
fn save_encoded_frame(encoded_frame: &[u8], index: usize) -> std::result::Result<(), std::io::Error> {
    let output_file = format!("/home/boris/Desktop/Rust_MG1/asynchronix/Video_Sink/frame{}.hevc", index);
    let mut file = File::create(output_file)?;
    file.write_all(encoded_frame)?;
    Ok(())
}

// Decode HEVC back to RGB (called every half second)
fn decode_hevc_to_rgb(encoded_frame: &[u8]) -> Result<Video, ffmpeg::Error> {
    let decoder = codec::decoder::find(codec::Id::HEVC)
        .ok_or(ffmpeg::Error::DecoderNotFound)?
        .video()?;

    let mut decoder_ctx = Context::new(codec::Id::HEVC)?;
    decoder_ctx.open_as(&decoder)?;

    let mut packet = ffmpeg::util::packet::Packet::empty();
    packet.set_data(encoded_frame);
    
    decoder_ctx.send_packet(&packet)?;

    let mut decoded_frame = Video::empty();
    decoder_ctx.receive_frame(&mut decoded_frame)?;

    Ok(decoded_frame)
}
