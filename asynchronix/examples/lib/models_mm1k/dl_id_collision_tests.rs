//! Two servers number their packets independently from 0, so their DL packets share packet_ids in the AP's one DL
//! deque. Sending one station's packet must not remove the other station's same-numbered packet (the "dead stream").
use super::*;
use asynchronix::ports::EventBuffer;
use asynchronix::simulation::{Mailbox, SimInit};

#[test]
fn sending_one_station_keeps_another_stations_same_numbered_packet() {
    let (servers, clients) = ([100, 101], [200, 201]);
    let link_configs = create_mlo_config("SLO80");
    let links: Vec<u8> = link_configs.iter().map(|lc| lc.link_id).collect();
    let ids: Vec<i32> = servers.iter().chain(clients.iter()).copied().collect();
    let mut queue = QueueModule::new(
        ids.len(), 0, 0.0, ids.clone(), "test_folder".to_string(), None, link_configs,
        LinkSelectionStrategy::Opportunistic, 1, "/tmp", None,
    );
    queue.sta_capabilities.insert(-1, StaCapabilities { _is_str_capable: true, links: links.clone() });
    for (i, id) in ids.iter().enumerate() {
        queue.sta_capabilities.insert(*id, StaCapabilities { _is_str_capable: true, links: links.clone() });
        queue.STA_coords_map.insert(*id as usize, Coords::with_coords(AP_X + 5.0 + i as f64, AP_Y, 0.0));
    }
    let mut out = EventBuffer::new();
    queue.link_outputs.get_mut(&0).unwrap().connect_sink(&out);
    let mbox = Mailbox::new();
    let addr = mbox.address();
    let mut simu = SimInit::new().add_model(queue, mbox, "queue").init(TaiTime::<0>::EPOCH);
    for k in 0..2 {
        let mut p = MpduPacket::new();
        p.sta_src_id = servers[k];
        p.sta_dest_id = clients[k];
        p.packet_id = 0; // the same number for both flows
        p.length_packet_bits = 8000;
        simu.process_event(QueueModule::input, p, addr.clone());
    }
    simu.step_by(Duration::from_millis(50));
    let mut got: Vec<i32> = std::iter::from_fn(|| out.next()).flat_map(|a| a.mpdu_packets.into_iter().map(|p| p.sta_dest_id)).collect();
    got.sort();
    if dl_packet_id_legacy() {
        return; // XRSIM_DL_ID_LEGACY=1 reproduces the defect on purpose
    }
    assert_eq!(got, vec![200, 201], "each station's packet 0 is delivered once");
}
