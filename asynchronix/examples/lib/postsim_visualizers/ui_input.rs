//! Where a window's mouse and keyboard come from. The viewers read their input through `UiInput`, which minifb's
//! `Window` implements by asking the window itself, so the post-sim viewer reads exactly what it always did. A
//! `FrameInput` is one frame's input as plain data (copied from a window, or built by hand in the tests), and two of them
//! merge into one, so the menu bar, the panels, the layout and the tour can be driven without a window.
//!
//! The method names and meanings are minifb's: positions in window pixels (the UI scale is applied by
//! `ui_scale::mouse_pos`), the wheel as minifb reports it (+1 per notch away from the user), and a key "pressed" in the
//! frame it went down (`KeyRepeat::Yes`: also each auto-repeat).

use minifb::{Key, KeyRepeat, MouseButton, MouseMode, Window};

/// A window's input, read the way minifb's `Window` reads it.
pub trait UiInput {
    fn get_mouse_pos(&self, mode: MouseMode) -> Option<(f32, f32)>;
    fn get_mouse_down(&self, button: MouseButton) -> bool;
    fn get_scroll_wheel(&self) -> Option<(f32, f32)>;
    fn is_key_down(&self, key: Key) -> bool;
    fn is_key_pressed(&self, key: Key, repeat: KeyRepeat) -> bool;
    fn get_keys_pressed(&self, repeat: KeyRepeat) -> Vec<Key>;
    fn get_size(&self) -> (usize, usize);
    fn get_position(&self) -> (isize, isize);
}

impl UiInput for Window {
    fn get_mouse_pos(&self, mode: MouseMode) -> Option<(f32, f32)> {
        Window::get_mouse_pos(self, mode)
    }
    fn get_mouse_down(&self, button: MouseButton) -> bool {
        Window::get_mouse_down(self, button)
    }
    fn get_scroll_wheel(&self) -> Option<(f32, f32)> {
        Window::get_scroll_wheel(self)
    }
    fn is_key_down(&self, key: Key) -> bool {
        Window::is_key_down(self, key)
    }
    fn is_key_pressed(&self, key: Key, repeat: KeyRepeat) -> bool {
        Window::is_key_pressed(self, key, repeat)
    }
    fn get_keys_pressed(&self, repeat: KeyRepeat) -> Vec<Key> {
        Window::get_keys_pressed(self, repeat)
    }
    fn get_size(&self) -> (usize, usize) {
        Window::get_size(self)
    }
    fn get_position(&self) -> (isize, isize) {
        Window::get_position(self)
    }
}

/// One frame's input, copied out of a window (`from_window`), built by hand, or two merged (`merge`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FrameInput {
    pub size: (usize, usize),
    pub position: (isize, isize),
    /// The pointer in window pixels, also outside the window (minifb's `MouseMode::Pass`); None: unknown.
    pub mouse: Option<(f32, f32)>,
    /// The pointer is over the window (`MouseMode::Discard` reports it only then).
    pub inside: bool,
    /// Left, middle, right.
    pub buttons: [bool; 3],
    pub wheel: (f32, f32),
    pub keys_down: Vec<Key>,
    /// Keys that went down this frame.
    pub pressed: Vec<Key>,
    /// The same plus this frame's auto-repeats.
    pub pressed_repeat: Vec<Key>,
}

fn button_index(b: MouseButton) -> usize {
    match b {
        MouseButton::Left => 0,
        MouseButton::Middle => 1,
        MouseButton::Right => 2,
    }
}

impl FrameInput {
    /// What `window` reports this frame.
    pub fn from_window(window: &Window) -> Self {
        FrameInput {
            size: window.get_size(),
            position: window.get_position(),
            mouse: window.get_mouse_pos(MouseMode::Pass),
            inside: window.get_mouse_pos(MouseMode::Discard).is_some(),
            buttons: [MouseButton::Left, MouseButton::Middle, MouseButton::Right].map(|b| window.get_mouse_down(b)),
            wheel: window.get_scroll_wheel().unwrap_or((0.0, 0.0)),
            keys_down: window.get_keys(),
            pressed: window.get_keys_pressed(KeyRepeat::No),
            pressed_repeat: window.get_keys_pressed(KeyRepeat::Yes),
        }
    }

    /// True if the pointer moved, a button changed, the wheel turned or a key went down since the frame `before` (the
    /// merged pointer follows whichever source did so last).
    pub fn active(&self, before: &FrameInput) -> bool {
        self.mouse != before.mouse || self.buttons != before.buttons || self.wheel != (0.0, 0.0) || !self.pressed_repeat.is_empty()
    }

    /// A local window's input and another source's for one frame: buttons, keys and the wheel from both, the pointer of
    /// `remote` while `remote_leads` (it moved last) or while the local pointer is away from the window, and the
    /// local window's size and place when there is one.
    pub fn merge(local: Option<FrameInput>, remote: Option<FrameInput>, remote_leads: bool) -> FrameInput {
        let (mut l, r) = match (local, remote) {
            (Some(l), Some(r)) => (l, r),
            (Some(l), None) => return l,
            (None, Some(r)) => return r,
            (None, None) => return FrameInput::default(),
        };
        if (remote_leads || !l.inside) && r.mouse.is_some() {
            (l.mouse, l.inside) = (r.mouse, r.inside);
        }
        for (a, b) in l.buttons.iter_mut().zip(r.buttons) {
            *a |= b;
        }
        l.wheel = (l.wheel.0 + r.wheel.0, l.wheel.1 + r.wheel.1);
        for (into, from) in [(&mut l.keys_down, r.keys_down), (&mut l.pressed, r.pressed), (&mut l.pressed_repeat, r.pressed_repeat)] {
            for k in from {
                if !into.contains(&k) {
                    into.push(k);
                }
            }
        }
        l
    }
}

impl UiInput for FrameInput {
    fn get_mouse_pos(&self, mode: MouseMode) -> Option<(f32, f32)> {
        let (x, y) = self.mouse?;
        match mode {
            MouseMode::Pass => Some((x, y)),
            MouseMode::Discard => self.inside.then_some((x, y)),
            MouseMode::Clamp => Some((x.clamp(0.0, self.size.0.saturating_sub(1) as f32), y.clamp(0.0, self.size.1.saturating_sub(1) as f32))),
        }
    }
    fn get_mouse_down(&self, button: MouseButton) -> bool {
        self.buttons[button_index(button)]
    }
    fn get_scroll_wheel(&self) -> Option<(f32, f32)> {
        (self.wheel != (0.0, 0.0)).then_some(self.wheel)
    }
    fn is_key_down(&self, key: Key) -> bool {
        self.keys_down.contains(&key)
    }
    fn is_key_pressed(&self, key: Key, repeat: KeyRepeat) -> bool {
        match repeat {
            KeyRepeat::No => self.pressed.contains(&key),
            KeyRepeat::Yes => self.pressed_repeat.contains(&key),
        }
    }
    fn get_keys_pressed(&self, repeat: KeyRepeat) -> Vec<Key> {
        match repeat {
            KeyRepeat::No => self.pressed.clone(),
            KeyRepeat::Yes => self.pressed_repeat.clone(),
        }
    }
    fn get_size(&self) -> (usize, usize) {
        self.size
    }
    fn get_position(&self) -> (isize, isize) {
        self.position
    }
}
