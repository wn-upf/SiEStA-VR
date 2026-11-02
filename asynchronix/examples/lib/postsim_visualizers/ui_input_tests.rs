//! `ui_input`: a frame's input read the way minifb reads a window, and the local window's input merged with a web
//! viewer's.

use super::ui_input::{FrameInput, UiInput};
use minifb::{Key, KeyRepeat, MouseButton, MouseMode};

fn local() -> FrameInput {
    FrameInput { size: (1880, 1000), position: (10, 20), mouse: Some((100.0, 50.0)), inside: true, ..Default::default() }
}

fn remote() -> FrameInput {
    FrameInput { size: (1880, 1000), mouse: Some((700.0, 300.0)), inside: true, ..Default::default() }
}

#[test]
fn frame_input_reads_like_a_window() {
    let mut f = local();
    f.buttons = [true, false, true];
    f.wheel = (0.0, -1.0);
    f.keys_down = vec![Key::Up];
    f.pressed = vec![Key::Space];
    f.pressed_repeat = vec![Key::Space, Key::Up];
    assert_eq!(f.get_mouse_pos(MouseMode::Discard), Some((100.0, 50.0)));
    assert!(f.get_mouse_down(MouseButton::Left) && !f.get_mouse_down(MouseButton::Middle) && f.get_mouse_down(MouseButton::Right));
    assert_eq!(f.get_scroll_wheel(), Some((0.0, -1.0)));
    assert!(f.is_key_down(Key::Up) && !f.is_key_down(Key::Space));
    // a key held: pressed only on its repeats
    assert!(f.is_key_pressed(Key::Space, KeyRepeat::No) && f.is_key_pressed(Key::Space, KeyRepeat::Yes));
    assert!(!f.is_key_pressed(Key::Up, KeyRepeat::No) && f.is_key_pressed(Key::Up, KeyRepeat::Yes));
    assert_eq!(f.get_keys_pressed(KeyRepeat::No), vec![Key::Space]);
    assert_eq!((f.get_size(), f.get_position()), ((1880, 1000), (10, 20)));
    // no wheel: None, as minifb
    assert_eq!(FrameInput::default().get_scroll_wheel(), None);
}

#[test]
fn a_pointer_outside_is_discarded_passed_or_clamped() {
    let f = FrameInput { size: (800, 600), mouse: Some((-5.0, 900.0)), inside: false, ..Default::default() };
    assert_eq!(f.get_mouse_pos(MouseMode::Discard), None);
    assert_eq!(f.get_mouse_pos(MouseMode::Pass), Some((-5.0, 900.0)));
    assert_eq!(f.get_mouse_pos(MouseMode::Clamp), Some((0.0, 599.0)));
    // ui_scale works on it as on a window (scale 1 on a test thread)
    assert_eq!(super::ui_scale::mouse_pos(&f), None);
    assert_eq!(super::ui_scale::mouse_pos_any(&f), Some((-5.0, 900.0)));
}

#[test]
fn merging_keeps_both_buttons_keys_and_wheels() {
    let mut l = local();
    l.buttons[2] = true;
    l.pressed = vec![Key::Tab];
    l.pressed_repeat = vec![Key::Tab];
    l.keys_down = vec![Key::Tab];
    l.wheel = (0.0, 1.0);
    let mut r = remote();
    r.buttons[0] = true;
    r.pressed = vec![Key::Space, Key::Tab];
    r.pressed_repeat = vec![Key::Space, Key::Tab];
    r.keys_down = vec![Key::Space];
    r.wheel = (0.0, 2.0);
    let m = FrameInput::merge(Some(l), Some(r), false);
    assert_eq!(m.buttons, [true, false, true]);
    assert_eq!(m.pressed, vec![Key::Tab, Key::Space]);
    assert_eq!(m.keys_down, vec![Key::Tab, Key::Space]);
    assert_eq!(m.wheel, (0.0, 3.0));
    // the local window's size and place
    assert_eq!((m.size, m.position), ((1880, 1000), (10, 20)));
}

#[test]
fn the_pointer_follows_whoever_moved_last() {
    // the local pointer over the window and the viewer quiet: the local one
    assert_eq!(FrameInput::merge(Some(local()), Some(remote()), false).mouse, Some((100.0, 50.0)));
    // the viewer moved last: theirs
    assert_eq!(FrameInput::merge(Some(local()), Some(remote()), true).mouse, Some((700.0, 300.0)));
    // the local pointer away from the window: the viewer's
    let away = FrameInput { inside: false, ..local() };
    let m = FrameInput::merge(Some(away), Some(remote()), false);
    assert_eq!((m.mouse, m.inside), (Some((700.0, 300.0)), true));
    // a viewer with no pointer yet changes nothing
    let none = FrameInput { mouse: None, inside: false, ..remote() };
    assert_eq!(FrameInput::merge(Some(local()), Some(none), true).mouse, Some((100.0, 50.0)));
    // one source alone is itself
    assert_eq!(FrameInput::merge(None, Some(remote()), false), remote());
    assert_eq!(FrameInput::merge(Some(local()), None, true), local());
}

#[test]
fn activity_is_a_move_a_button_change_a_wheel_or_a_key() {
    let before = local();
    assert!(!local().active(&before));
    assert!(FrameInput { mouse: Some((101.0, 50.0)), ..local() }.active(&before));
    assert!(FrameInput { buttons: [true, false, false], ..local() }.active(&before));
    assert!(FrameInput { wheel: (0.0, 1.0), ..local() }.active(&before));
    assert!(FrameInput { pressed_repeat: vec![Key::A], ..local() }.active(&before));
    // a key only held is not news
    assert!(!FrameInput { keys_down: vec![Key::A], ..local() }.active(&before));
}
