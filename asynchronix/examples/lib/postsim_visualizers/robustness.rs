//! Tests that the unified viewer's layout maths survives any window size, however a drag or the window manager shrinks it.

use super::*;

#[test]
fn fill_rect_never_panics() {
    for stride in [1usize, 5, 40] {
        let mut buf = vec![0u32; stride * 7];
        for x in [0usize, 1, stride - 1, stride, stride + 3, 10_000] {
            for y in [0usize, 3, 6, 7, 9_999] {
                for (w, h) in [(0usize, 0usize), (1, 1), (stride, 9), (100_000, 100_000)] {
                    fill_rect(&mut buf, stride, x, y, w, h, 0xff);
                }
            }
        }
    }
    let mut empty: Vec<u32> = Vec::new();
    fill_rect(&mut empty, 0, 3, 3, 4, 4, 1);
}

#[test]
fn splitter_limits_stay_ordered_and_inside() {
    for w in 0..4000usize {
        let (lo, hi) = split_limits(w);
        assert!(lo <= hi, "w={w}");
        if !window_too_small(w, MIN_FRAME_H) {
            assert!(hi <= w, "w={w}");
            // room for the channel plot on the left and for the ABR sidebar and plot on the right
            assert!(lo >= 240 + CH_MIN_PLOT_W && w - hi >= 400 || hi == lo, "w={w}");
        }
    }
}

#[test]
fn small_windows_are_skipped() {
    assert!(window_too_small(0, 0));
    assert!(window_too_small(MIN_FRAME_W - 1, 2000));
    assert!(window_too_small(2000, MIN_FRAME_H - 1));
    assert!(!window_too_small(MIN_FRAME_W, MIN_FRAME_H));
    // the layout itself must hold at the smallest drawn size
    let (lo, hi) = split_limits(MIN_FRAME_W);
    assert!(lo <= hi && hi <= MIN_FRAME_W);
}

#[test]
fn bottom_panels_sizes_are_finite_at_any_size() {
    let r = panel_resize::PanelResize::default();
    for bottom in [0usize, 10, 100, 400, 1500] {
        for lanes in [0usize, 1, 3] {
            let g = panel_resize::Geometry { right: 500, bottom, base_const: 60, bodies: vec![30; lanes], below_lanes: 40 };
            for cwd in [0usize, 48, 200] {
                let s = r.sizes(cwd, &g);
                assert!(s.f.is_finite() && s.f > 0.0);
                assert!(s.cw_h < 100_000 && s.q_h < 100_000);
            }
        }
    }
}

#[test]
fn sidebar_sizes_never_panic() {
    let r = sidebar_resize::SidebarResize::default();
    for room_w in [0usize, 100, 400, 3000] {
        for bottom in [0usize, 50, 900] {
            let g = sidebar_resize::Geometry { left: 700, room_w, top: 60, bottom, has_map: true, legend_h: 150, on_right: room_w == 400 };
            let (sw, mh) = r.sizes(260, 180, &g);
            assert!(sw < 100_000 && mh < 100_000);
        }
    }
}

#[test]
fn zoomed_in_ticks_show_different_numbers() {
    assert_eq!(tick_label(1.5, 0.5), "1.50s");
    assert_eq!(tick_label(12.0, 10.0), "12.00s");
    for exp in 0..=9 {
        let step = 10f64.powi(-exp);
        for mult in [1.0, 2.0, 5.0] {
            let step = step * mult;
            let t0 = 3.0 + 7.0 * step;
            let a = tick_label(t0, step);
            let b = tick_label(t0 + step, step);
            assert_ne!(a, b, "step {step}: {a} == {b}");
        }
    }
    assert_eq!(tick_label(1.0, 0.0), "1.00s");
    assert_eq!(tick_label(1.0, f64::NAN), "1.00s");
}
