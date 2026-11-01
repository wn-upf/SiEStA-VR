//! Order of actions scheduled for the same time on different mailboxes.

use std::time::Duration;

use asynchronix::model::Model;
use asynchronix::ports::{EventBuffer, Output};
use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

struct Tag {
    output: Output<usize>,
}
impl Tag {
    async fn input(&mut self, id: usize) {
        self.output.send(id).await;
    }
}
impl Model for Tag {}

/// Same-time actions on different mailboxes run in an order fixed by mailbox creation order (the reverse of it:
/// the single-threaded executor pops its work queue LIFO), whatever the mailboxes' heap addresses and the order
/// the actions were scheduled in. Ordering by address made the outcome depend on the allocator's state (CPU
/// count, malloc arenas, threads started before setup).
#[test]
fn same_time_actions_run_in_mailbox_creation_order() {
    // Free every other mailbox before creating more, so later mailboxes reuse earlier, interleaved slots.
    let mut boxes: Vec<(usize, Mailbox<Tag>)> = (0..64).map(|i| (i, Mailbox::new())).collect();
    boxes = boxes.into_iter().filter(|(i, _)| i % 2 == 0).collect();
    boxes.extend((64..96).map(|i| (i, Mailbox::new())));

    let out = EventBuffer::with_capacity(256);
    let mut init = SimInit::with_num_threads(1);
    let mut addrs = Vec::new();
    for (i, mbox) in boxes {
        let mut model = Tag { output: Output::default() };
        model.output.connect_sink(&out);
        addrs.push((i, mbox.address()));
        init = init.add_model(model, mbox, "");
    }
    let t0 = MonotonicTime::EPOCH;
    let mut simu = init.init(t0);
    let scheduler = simu.scheduler();
    for (i, addr) in addrs.iter().rev() {
        scheduler.schedule_event(Duration::from_secs(1), Tag::input, *i, addr).unwrap();
    }
    simu.step();

    let got: Vec<usize> = out.collect();
    let want: Vec<usize> = (0..64).step_by(2).chain(64..96).rev().collect();
    assert_eq!(got, want);
}
