#!/bin/bash


input_video="/home/boris/Desktop/Rust_MG1/asynchronix/video_samples_vmaf/bbb_1080p60fps.mp4"

mkdir dbg_output_frames


# Extract frames with NVIDIA hardware-accelerated HEVC encoding
ffmpeg -hwaccel cuda -hwaccel_output_format cuda \
  -i $input_video \
  -c:v hevc_nvenc \
  -b:v 2M \
  -preset p4 \
  -tune hq \
  -x265-params "keyint=1:min-keyint=1" \
  -frame_pts 1 \
  dbg_output_frames/frame_%d.hevc