use crate::debug_bgprint;
use crate::lib::alvr_packets::ClientStatistics;
use crate::lib::alvr_packets::NetworkStatisticsPacket;
use crate::lib::SlidingWindowAverage;

use crate::lib::{
    EventType, GraphNetworkStatistics, GraphNetworkStatistics_csv, NominalBitrateStats,
    SlidingWindowTimely, SlidingWindowWeighted,
};
use crate::DebugColor;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::Path;
// use ::{warn, SlidingWindowAverage};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};
use tai_time::{TaiClock, TaiTime};

#[derive(Clone)]
struct HistoryFrame {
    input_acquired: Instant,
    video_packet_received: Instant,
    client_stats: ClientStatistics,

    is_decoded: bool,
    is_composed: bool,
    is_submitted: bool,
}
#[derive(Default, Clone)]
struct BatteryData {
    gauge_value: f32,
    is_plugged: bool,
}

pub struct StatisticsManager {
    history_buffer: VecDeque<HistoryFrame>,
    max_history_size: usize,

    last_full_report_instant: Instant,
    last_nominal_bitrate_stats: NominalBitrateStats,

    last_frame_present_instant: Instant,
    last_frame_present_interval: Duration,

    last_vsync_time: Instant,

    video_packets_total: usize,
    video_packets_partial_sum: usize,

    video_bytes_total: usize,
    video_bytes_partial_sum: usize,

    received_video_bytes_partial_sum: f32,

    frame_interarrival_partial_sum: f32,

    packets_dropped_total: usize,
    packets_dropped_partial_sum: usize,

    packets_skipped_total: usize,
    packets_skipped_partial_sum: usize,

    battery_gauges: HashMap<u64, BatteryData>,
    steamvr_pipeline_latency: Duration,

    // Latency metrics
    total_pipeline_latency_average: SlidingWindowAverage<Duration>,
    game_delay_average: SlidingWindowAverage<Duration>,
    server_compositor_average: SlidingWindowAverage<Duration>,
    encode_delay_average: SlidingWindowAverage<Duration>,
    network_delay_average: SlidingWindowAverage<Duration>,
    decode_delay_average: SlidingWindowAverage<Duration>,
    decoder_queue_delay_average: SlidingWindowAverage<Duration>,
    client_compositor_average: SlidingWindowAverage<Duration>,
    vsync_queue_delay_average: SlidingWindowAverage<Duration>,

    frame_interval: Duration,

    frame_interval_average: SlidingWindowAverage<Duration>,
    client_frame_interval_average: SlidingWindowAverage<Duration>,

    frame_interarrival_average: SlidingWindowAverage<f32>,

    server_frames_moving: SlidingWindowTimely<f32>,
    client_frames_moving: SlidingWindowTimely<f32>,

    history_throughput_weighted: SlidingWindowWeighted<f32>,
    interval_avg_plot_throughput: f32,
    instant_weighted_avg_prev: TaiTime<0>,

    prev_highest_shard: i32,
    prev_highest_frame: i32,

    stats_history_buffer: VecDeque<HistoryFrame>,
    map_frames_spf: HashMap<u32, usize>,

    is_first_stats: bool,

    folder: String,
    last_stats: GraphNetworkStatistics_csv,
}

impl StatisticsManager {
    pub fn new(
        max_history_size: usize,
        nominal_server_frame_interval: Duration,
        steamvr_pipeline_frames: f32,
        folder: &str,
    ) -> Self {
        Self {
            history_buffer: VecDeque::new(),
            max_history_size,

            last_full_report_instant: Instant::now(),
            last_nominal_bitrate_stats: NominalBitrateStats::default(),

            last_frame_present_instant: Instant::now(),
            last_frame_present_interval: Duration::ZERO,

            last_vsync_time: Instant::now(),

            video_packets_total: 0,
            video_packets_partial_sum: 0,

            video_bytes_total: 0,
            video_bytes_partial_sum: 0,

            received_video_bytes_partial_sum: 0.,

            frame_interarrival_partial_sum: 0.,

            packets_dropped_total: 0,
            packets_dropped_partial_sum: 0,

            packets_skipped_total: 0,
            packets_skipped_partial_sum: 0,

            battery_gauges: HashMap::new(),
            steamvr_pipeline_latency: Duration::from_secs_f32(
                steamvr_pipeline_frames * nominal_server_frame_interval.as_secs_f32(),
            ),

            total_pipeline_latency_average: SlidingWindowAverage::new(
                Duration::ZERO,
                max_history_size,
            ),
            game_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            server_compositor_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            encode_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            network_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            decode_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            decoder_queue_delay_average: SlidingWindowAverage::new(
                Duration::ZERO,
                max_history_size,
            ),
            client_compositor_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),
            vsync_queue_delay_average: SlidingWindowAverage::new(Duration::ZERO, max_history_size),

            frame_interval: nominal_server_frame_interval,

            frame_interval_average: SlidingWindowAverage::new(
                Duration::from_millis(16),
                max_history_size,
            ),
            client_frame_interval_average: SlidingWindowAverage::new(
                Duration::from_millis(16),
                max_history_size,
            ),

            frame_interarrival_average: SlidingWindowAverage::new(0., max_history_size),

            server_frames_moving: SlidingWindowTimely::new(60., 16., 1.),
            client_frames_moving: SlidingWindowTimely::new(60., 16., 1.),

            history_throughput_weighted: SlidingWindowWeighted::new(0., 0.0),
            instant_weighted_avg_prev: TaiTime::EPOCH,
            interval_avg_plot_throughput: 0. as f32,

            prev_highest_shard: -1,
            prev_highest_frame: 0,

            stats_history_buffer: VecDeque::new(),
            map_frames_spf: HashMap::new(),

            is_first_stats: true,

            folder: folder.to_string(),
            last_stats: GraphNetworkStatistics_csv::default(),
        }
    }
    // This statistics are reported for every succesfully received frame
    pub fn report_network_statistics(
        &mut self,
        network_stats: NetworkStatisticsPacket,
        rtt: Duration,
        now: TaiTime<0>,
    ) -> (f32, f32) {
        self.packets_skipped_total += network_stats.frames_skipped as usize;
        self.packets_skipped_partial_sum += network_stats.frames_skipped as usize;

        self.received_video_bytes_partial_sum += network_stats.rx_bytes as f32;

        self.frame_interarrival_partial_sum += network_stats.frame_interarrival;

        let mut frame_interarrival = network_stats.frame_interarrival;
        if !self.is_first_stats {
            self.frame_interarrival_average
                .submit_sample(frame_interarrival);
        } else {
            frame_interarrival = 7.0;
            self.is_first_stats = false;
        }

        let peak_network_throughput_bps: f32 = if network_stats.frame_span != 0.0 {
            network_stats.bytes_in_frame as f32 * 8.0 / network_stats.frame_span
        } else {
            0.0
        };

        let instant_network_throughput_bps: f32 = if network_stats.frame_interarrival != 0.0 {
            network_stats.rx_bytes as f32 * 8.0 / network_stats.frame_interarrival
        } else {
            0.0
        };

        self.history_throughput_weighted.submit_sample(
            instant_network_throughput_bps,
            network_stats.frame_interarrival,
        );

        let mut shards_sent: usize = 0;
        let shards_lost: isize;

        if self.prev_highest_frame == network_stats.highest_rx_frame_index as i32 {
            if self.prev_highest_shard < network_stats.highest_rx_shard_index as i32 {
                shards_sent =
                    (network_stats.highest_rx_shard_index - self.prev_highest_shard) as usize;

                self.prev_highest_shard = network_stats.highest_rx_shard_index as i32;
            }
        } else if self.prev_highest_frame < network_stats.highest_rx_frame_index as i32 {
            let shards_from_prev = match self.map_frames_spf.get(&(self.prev_highest_frame as u32))
            {
                Some(&shards_count_prev) => {
                    shards_count_prev.saturating_sub((self.prev_highest_shard + 1) as usize)
                }
                None => 0,
            };

            let shards_from_inbetween: usize = self
                .map_frames_spf
                .iter()
                .filter(|&(frame, _)| {
                    *frame > self.prev_highest_frame as u32
                        && *frame < network_stats.highest_rx_frame_index as u32
                })
                .map(|(_, val)| *val)
                .sum();

            let shards_from_actual = network_stats.highest_rx_shard_index as usize + 1;
            shards_sent = shards_from_prev + shards_from_inbetween + shards_from_actual;
        }

        shards_lost = shards_sent as isize - network_stats.rx_shard_counter as isize;

        self.prev_highest_frame = network_stats.highest_rx_frame_index as i32;
        self.prev_highest_shard = network_stats.highest_rx_shard_index as i32;

        let keys_to_drop: Vec<_> = self
            .map_frames_spf
            .iter()
            .filter(|&(frame, _)| *frame < self.prev_highest_frame as u32)
            .map(|(key, _)| *key)
            .collect();

        for key in keys_to_drop {
            self.map_frames_spf.remove_entry(&key);
        }

        if now.duration_since(self.instant_weighted_avg_prev) >= Duration::from_secs(1) {
            self.instant_weighted_avg_prev = now;
            self.interval_avg_plot_throughput = self.history_throughput_weighted.get_average();
        }

        debug_bgprint!(
            DebugColor::Magenta,
            "[DBG STATS XR] reporting frame {}",
            network_stats.frame_index
        );

        self.last_stats = GraphNetworkStatistics_csv {
            timestamp: now
                .checked_duration_since(TaiTime::EPOCH)
                .unwrap()
                .as_secs_f64(),
            frame_index: network_stats.frame_index as usize,

            frame_size_bytes: network_stats.bytes_in_frame as usize,

            server_fps: 1.
                / self
                    .server_frames_moving
                    .get_interval_buffer_mean()
                    .max(Duration::from_millis(1).as_secs_f32()),

            client_fps: 1.
                / self
                    .client_frames_moving
                    .get_interval_buffer_mean()
                    .max(Duration::from_millis(1).as_secs_f32()),

            frame_span_ms: network_stats.frame_span * 1000.0,

            interarrival_jitter_ms: network_stats.interarrival_jitter * 1000.0,

            ow_delay_ms: network_stats.ow_delay * 1000.0,
            filtered_ow_delay_ms: network_stats.filtered_ow_delay * 1000.0,

            rtt_ms: rtt.as_secs_f32() * 1000.0,

            frame_interarrival_ms: network_stats.frame_interarrival * 1000.0,
            frame_jitter_ms: self.frame_interarrival_average.get_std() * 1000.0,

            frames_skipped: network_stats.frames_skipped,

            shards_lost: shards_lost,
            shards_duplicated: network_stats.duplicated_shard_counter,

            instant_network_throughput_bps: instant_network_throughput_bps,
            peak_network_throughput_bps: peak_network_throughput_bps,

            requested_bps: self.last_nominal_bitrate_stats.requested_bps.clone(),

            interval_avg_plot_throughput: self.interval_avg_plot_throughput,
        };

        debug_bgprint!(DebugColor::Magenta, "\t{:#?}", self.last_stats);

        // Call method to save data to CSV
        if self.save_network_stats_to_csv().is_err() {
            println!("ERROR HERE CSV!!");
        }
        return (peak_network_throughput_bps, frame_interarrival);
    }
    // Add a method to save stats to CSV
    pub fn save_network_stats_to_csv(&self) -> io::Result<()> {
        let file_path = format!("Results/{}/XR_stats.csv", self.folder);
        let path = Path::new(&file_path);

        // Open the CSV file in append mode or create it if it doesn't exist
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;

        // Prepare the header if the file is empty
        if file.metadata()?.len() == 0 {
            writeln!(
                file,
                "timestamp,frame_index,frame_size_bytes,server_fps,client_fps,frame_span_ms,interarrival_jitter_ms,ow_delay_ms,filtered_ow_delay_ms,rtt_ms,frame_interarrival_ms,frame_jitter_ms,frames_skipped,shards_lost,shards_duplicated,instant_network_throughput_bps,peak_network_throughput_bps,nominal_bitrate,interval_avg_plot_throughput"
            )?;
        }

        // Prepare the data line to write to the CSV
        let data_line = format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.last_stats.timestamp,                      // frame_index
            self.last_stats.frame_index,                    // frame_index
            self.last_stats.frame_size_bytes,               // frame_size_bytes
            self.last_stats.server_fps,                     // server_fps
            self.last_stats.client_fps,                     // client_fps
            self.last_stats.frame_span_ms,                  // frame_span_ms
            self.last_stats.interarrival_jitter_ms,         // interarrival_jitter_ms
            self.last_stats.ow_delay_ms,                    // ow_delay_ms
            self.last_stats.filtered_ow_delay_ms,           // filtered_ow_delay_ms
            self.last_stats.rtt_ms,                         // rtt_ms
            self.last_stats.frame_interarrival_ms,          // frame_interarrival_ms
            self.last_stats.frame_jitter_ms,                // frame_jitter_ms
            self.last_stats.frames_skipped,                 // frames_skipped
            self.last_stats.shards_lost,                    // shards_lost
            self.last_stats.shards_duplicated,              // shards_duplicated
            self.last_stats.instant_network_throughput_bps, // instant_network_throughput_bps
            self.last_stats.peak_network_throughput_bps,    // peak_network_throughput_bps
            self.last_stats.requested_bps,                  // nominal_bitrate
            self.interval_avg_plot_throughput,              // interval_avg_plot_throughput
        );

        // Write the data line to the CSV file
        writeln!(file, "{}", data_line)?;

        Ok(())
    }

    pub fn report_input_acquired(&mut self, target_timestamp: Duration) {
        if !self
            .history_buffer
            .iter()
            .any(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            self.history_buffer.push_front(HistoryFrame {
                input_acquired: Instant::now(),
                // this is just a placeholder because Instant does not have a default value
                video_packet_received: Instant::now(),
                client_stats: ClientStatistics {
                    target_timestamp,
                    frame_index: -1,
                    ..Default::default()
                },
                is_decoded: false,
                is_composed: false,
                is_submitted: false,
            });
        }

        if self.history_buffer.len() > self.max_history_size {
            self.history_buffer.pop_back();
        }
    }

    pub fn report_video_packet_received(&mut self, target_timestamp: Duration) {
        if let Some(frame) = self
            .history_buffer
            .iter_mut()
            .find(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            frame.video_packet_received = Instant::now();
            self.stats_history_buffer.push_back(frame.clone());

            if self.stats_history_buffer.len() > self.max_history_size {
                self.stats_history_buffer.pop_front();
            }
        }
    }

    pub fn report_video_packet_data(
        &mut self,
        target_timestamp: Duration,
        frame_index: u32,
        frames_dropped: u32,
    ) {
        if let Some(frame) = self.stats_history_buffer.iter_mut().find(|frame| {
            frame.client_stats.target_timestamp == target_timestamp
                && frame.client_stats.frame_index == -1
        }) {
            frame.client_stats.frame_index = frame_index as i32;
            frame.client_stats.frames_dropped = frames_dropped;
        }
    }

    pub fn report_video_packet_dropped(&mut self, frame_index: u32) {
        if let Some(index) = self
            .stats_history_buffer
            .iter()
            .position(|frame| frame.client_stats.frame_index == frame_index as i32)
        {
            self.stats_history_buffer.remove(index);
        }
    }
    pub fn report_frame_decoded(&mut self, target_timestamp: Duration) {
        if let Some(frame) = self.stats_history_buffer.iter_mut().find(|frame| {
            frame.client_stats.target_timestamp == target_timestamp && !frame.is_decoded
        }) {
            frame.is_decoded = true;

            frame.client_stats.video_decode =
                Instant::now().saturating_duration_since(frame.video_packet_received);
        }
    }

    pub fn report_compositor_start(&mut self, target_timestamp: Duration) {
        if let Some(frame) = self.stats_history_buffer.iter_mut().find(|frame| {
            frame.client_stats.target_timestamp == target_timestamp && !frame.is_composed
        }) {
            frame.is_composed = true;

            frame.client_stats.video_decoder_queue = Instant::now().saturating_duration_since(
                frame.video_packet_received + frame.client_stats.video_decode,
            );
        }
    }
    // // vsync_queue is the latency between this call and the vsync. it cannot be measured by ALVR and
    // // should be reported by the VR runtime
    // pub fn report_submit(&mut self, target_timestamp: Duration, vsync_queue: Duration) {
    //     let now = Instant::now();

    //     if let Some(frame) = self.stats_history_buffer.iter_mut().find(|frame| {
    //         frame.client_stats.target_timestamp == target_timestamp && !frame.is_submitted
    //     }) {
    //         frame.is_submitted = true;
    //         frame.client_stats.rendering = now.saturating_duration_since(
    //             frame.video_packet_received
    //                 + frame.client_stats.video_decode
    //                 + frame.client_stats.video_decoder_queue,
    //         );
    //         frame.client_stats.vsync_queue = vsync_queue;
    //         frame.client_stats.total_pipeline_latency =
    //             now.saturating_duration_since(frame.input_acquired) + vsync_queue;
    //         self.total_pipeline_latency_average
    //             .submit_sample(frame.client_stats.total_pipeline_latency);

    //         let vsync = now + vsync_queue;
    //         frame.client_stats.frame_interval = vsync.saturating_duration_since(self.prev_vsync);
    //         self.prev_vsync = vsync;
    //     }
    // }

    pub fn summary(&mut self, target_timestamp: Duration) -> Option<ClientStatistics> {
        if let Some(index) = self
            .stats_history_buffer
            .iter()
            .position(|frame| frame.client_stats.target_timestamp == target_timestamp)
        {
            if let Some(frame) = self.stats_history_buffer.remove(index) {
                let mut frame_client_stats_clone = frame.client_stats.clone();

                // a previously decoded frame can have been dropped after decoding
                self.stats_history_buffer.retain(|frame_dropped| {
                    if frame_dropped.client_stats.target_timestamp < target_timestamp {
                        println!(
                            "Dropped video packet {}. Reason: ??",
                            frame_dropped.client_stats.frame_index
                        ); // TODO: find the reason
                        frame_client_stats_clone.frames_dropped +=
                            frame_dropped.client_stats.frames_dropped + 1;
                        false
                    } else {
                        true
                    }
                });
                Some(frame_client_stats_clone)
            } else {
                None
            }
        } else {
            None
        }
    }

    // latency used for head prediction
    pub fn average_total_pipeline_latency(&self) -> Duration {
        self.total_pipeline_latency_average.get_average()
    }

    // latency used for controllers/trackers prediction
    pub fn tracker_prediction_offset(&self) -> Duration {
        self.total_pipeline_latency_average
            .get_average()
            .saturating_sub(self.steamvr_pipeline_latency)
    }
}
