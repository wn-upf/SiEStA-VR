#!/usr/bin/env python3
"""Regenerates the README's figures of the post-simulation viewer (assets/viz_readme/*.png).

    python3 scripts/viz_readme_figs.py [--bin target/release/examples/XR_sim] [--out assets/viz_readme]

Needs a release build of XR_sim and Pillow. It runs the simulation below several times with the viewer on
(XRSIM_VIZ=1): once to dump the run's TXOPs, collisions and the quick tour's examples (XRSIM_VIZ_EVENTS), then once per
figure with the channel view set on an instant picked from them (XRSIM_VIZ_VIEW), saving the first frame and where the
viewer drew each part (XRSIM_VIZ_SHOT) and closing the window (XRSIM_VIZ_EXIT). It then crops the frames and draws the
numbered markers. With xvfb-run installed the windows open on a virtual display; otherwise each one flashes up and
closes by itself. Same seed, same events: the instants are the same on every run.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent

# 20 s, 6 VR users with NeSt-VR (abr 1), MLO over two 80 MHz links and one 320 MHz link, seed 1 (the paper's command
# line, see scripts/byteexact_gate.sh args_of). {out} is the results folder.
RUN_ARGS = ("20.0 12000.0 5.0 100.0 0.1 6 0 5000 0 STD snow_short 90.0 0 1.5 1 30 1 0 1 1 1 0 1 1 0 1.0 "
            "MLO80-80-320 0 1 64 HEVC {out} 0 0 1")
# logical frame 1760 x 760 for the default 2200 x 950 window
UI_SCALE = "1.25"

SLOT = 9e-6
SIFS = 16e-6
LEGACY_PHY = 20e-6

MARK_R = 11
MARK_FILL = (214, 40, 80)
FONT_PATHS = ["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
              "/Library/Fonts/Arial Bold.ttf", "C:/Windows/Fonts/arialbd.ttf"]


def font(size):
    for p in FONT_PATHS:
        if os.path.exists(p):
            return ImageFont.truetype(p, size)
    return ImageFont.load_default(size=size)


def run_viewer(binary, env_extra, work):
    """One simulation with the viewer on; returns when its window has closed."""
    out = tempfile.mkdtemp(dir=work) + "/"
    env = dict(os.environ, XRSIM_VIZ="1", XRSIM_VIZ_EXIT="1", XRSIM_VIZ_SCALE=UI_SCALE, **env_extra)
    cmd = [str(binary)] + RUN_ARGS.format(out=out).split()
    if shutil.which("xvfb-run"):
        cmd = ["xvfb-run", "-a", "-s", "-screen 0 2560x1440x24"] + cmd
    res = subprocess.run(cmd, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if res.returncode != 0:
        sys.exit(f"XR_sim failed ({res.returncode}):\n{res.stdout[-3000:]}")
    shutil.rmtree(out, ignore_errors=True)


def shot(binary, work, name, center, span, cursor=None):
    """The viewer's first frame on the channel view (center, span) in s, and its layout."""
    prefix = os.path.join(work, name)
    view = f"{center},{span * 1e3}" + (f",{cursor}" if cursor is not None else "")
    run_viewer(binary, {"XRSIM_VIZ_VIEW": view, "XRSIM_VIZ_SHOT": prefix}, work)
    with open(prefix + ".json") as f:
        layout = json.load(f)
    return Image.open(prefix + ".png").convert("RGB"), layout


def x_of(t, lay):
    t0 = lay["center"] - lay["span"] / 2
    return lay["panel_x"] + (t - t0) / lay["span"] * lay["panel_w"]


def lane(lay, link):
    return next(l for l in lay["lanes"] if l["link"] == link)


def marker(draw, x, y, n, r=MARK_R):
    """A numbered marker: filled circle, white ring, dark outer edge (legible on light and dark pages)."""
    x, y = round(x), round(y)
    draw.ellipse((x - r - 2, y - r - 2, x + r + 2, y + r + 2), fill=(20, 20, 28))
    draw.ellipse((x - r - 1, y - r - 1, x + r + 1, y + r + 1), fill=(255, 255, 255))
    draw.ellipse((x - r + 1, y - r + 1, x + r - 1, y + r - 1), fill=MARK_FILL)
    f = font(13 if n < 10 else 11)
    draw.text((x, y + 1), str(n), fill=(255, 255, 255), font=f, anchor="mm")


def save(img, path):
    img.save(path, optimize=True)
    if os.path.getsize(path) > 400_000:
        img.quantize(colors=128, method=Image.Quantize.MEDIANCUT).save(path, optimize=True)
    print(f"{path}: {img.size[0]}x{img.size[1]}, {os.path.getsize(path) // 1024} KB")


def with_band(img, scale, band):
    """`img` scaled by `scale` (nearest, the viewer's pixel font stays sharp) under a dark band of `band` px."""
    big = img.resize((img.width * scale, img.height * scale), Image.NEAREST)
    canvas = Image.new("RGB", (big.width, big.height + band), (13, 13, 18))
    canvas.paste(big, (0, band))
    return canvas


# ── Picking the instants (as postsim_visualizers/tour_examples.rs does: from the middle of the run) ────────────────

def from_mid(items, key, mid):
    return sorted(items, key=lambda e: (key(e) < mid, abs(key(e) - mid)))


def pick_txop(ev):
    """A TXOP whose every part is readable: AIFS, then 3 to 8 backoff slots counted right after it, a short data block.
    On LINK 1: LINK 0's lane has the channel side's ZOOM and RESET buttons over its top right."""
    mid = (ev["t_min"] + ev["t_max"]) / 2
    for x in from_mid(ev["txops"], lambda e: e["t"], mid):
        a = x["access"]
        if not a or x["rts"] <= 0:
            continue
        (a0, a1), (b0, b1), slots = a["aifs"], a["bo"], a["slots"]
        data = x["data"][1] - x["data"][0]
        if 3 <= slots <= 8 and b0 <= a1 + SLOT and abs(b1 - x["t"]) < 1e-6 and 30e-6 <= data <= 120e-6 and x["link"] == 1:
            return x
    sys.exit("no TXOP with a readable countdown and data block")


def pick_collision(ev):
    """A collision of two contenders with TXOPs on its link just before and after it."""
    mid = (ev["t_min"] + ev["t_max"]) / 2
    by_link = {}
    for x in ev["txops"]:
        by_link.setdefault(x["link"], []).append(x)
    for c in from_mid(ev["collisions"], lambda e: e["t"], mid):
        if len(c["contenders"]) != 2:
            continue
        near = [x for x in by_link.get(c["link"], []) if c["t"] - 250e-6 < x["end"] <= c["t"] or c["end"] <= x["t"] < c["end"] + 250e-6]
        if len(near) >= 2:
            return c
    sys.exit("no two-contender collision")


def example(ev, kind):
    e = next((e for e in ev["examples"] if e["kind"] == kind), None)
    if e is None:
        sys.exit(f"the run has no {kind} example")
    return e


def who(sta):
    return "the AP" if sta < 0 else f"s{sta}"


# ── The figures ───────────────────────────────────────────────────────────────

def fig_overview(binary, work, ev, coll, out):
    span = 1.6e-3
    img, lay = shot(binary, work, "overview", coll["t"] + 0.25e-3, span)
    d = ImageDraw.Draw(img)
    reg = lay["regions"]
    lanes = lay["lanes"]
    marks = []
    marks.append((1, 392, reg["menu"][1] + reg["menu"][3] / 2))
    marks.append((2, 805, reg["controls"][1] + reg["controls"][3] / 2))
    marks.append((3, lay["panel_x"] - 16, lanes[0]["y"] + lanes[0]["h"] / 2))
    # 4: the parts of a TXOP fully in view on LINK 0
    t_lo, t_hi = lay["center"] - lay["span"] / 2, lay["center"] + lay["span"] / 2
    l0 = lane(lay, 0)
    shown = [x for x in ev["txops"] if x["link"] == 0 and x["rts"] > 0 and x["access"] and
             x["access"]["aifs"][0] > t_lo + 0.05 * span and x["end"] < t_hi - 0.05 * span and x["t"] > lay["cursor"]]
    if shown:
        x = shown[0]
        marks.append((4, x_of(x["t"] + x["rts"] + SIFS + x["cts"] / 2, lay), l0["y"] + l0["h"] - 12))
    lc = lane(lay, coll["link"])
    marks.append((5, x_of((coll["t"] + coll["end"]) / 2, lay), lc["y"] + lc["h"] / 2))
    marks.append((6, x_of(lay["cursor"], lay) + 14, lanes[-1]["y"] + lanes[-1]["h"] + 10))
    for n, key in [(7, "cw"), (8, "queues")]:
        r = reg[key]
        marks.append((n, r[0] + r[2] - 18, r[1] + 12))
    lg = reg["legend"]
    n_sessions = 6
    marks.append((9, lg[0] + lg[2] - 16, lg[1] + 12))
    marks.append((10, lg[0] + lg[2] - 16, lg[1] + 16 * n_sessions + 42))
    mp = reg["map"]
    marks.append((11, mp[0] + mp[2] - 18, mp[1] + 30))
    strips = {s["name"].split(" [")[0]: s["rect"] for s in lay["abr_strips"]}
    for n, names in [(12, ["Bitrate"]), (13, ["Peak Throughput", "Instant Throughput"]), (14, ["RTT"]),
                     (15, ["Frame-Loss Ratio"])]:
        for name in names:
            r = strips[name]
            marks.append((n, r[0] + r[2] - 120, r[1] + 16))
    ta = reg["time_axis"]
    marks.append((16, 120, ta[1] + ta[3] / 2))
    for n, x, y in marks:
        marker(d, x, y, n)
    save(img, out / "overview.png")


def fig_txop(binary, work, ev, x, out):
    a = x["access"]
    a0, end = a["aifs"][0], x["end"]
    pad = 0.05 * (end - a0)
    span = (end - a0) + 2 * pad
    img, lay = shot(binary, work, "txop", (a0 + end) / 2, span, cursor=a0 - pad / 2)
    ln = lane(lay, x["link"])
    xa, xb = int(x_of(a0 - pad * 0.8, lay)), int(x_of(end + pad * 0.8, lay))
    crop = img.crop((xa, ln["y"] - 2, xb, ln["y"] + ln["h"] + 2))
    scale = 2
    band = 40
    fig = with_band(crop, scale, band)
    d = ImageDraw.Draw(fig)
    t, rts, cts, ba = x["t"], x["rts"], x["cts"], x["ba"]
    d0, d1 = x["data"]
    p0 = d0 - x["preamble"]
    parts = [(1, a["aifs"][0], a["aifs"][1]), (2, a["bo"][0], a["bo"][1]), (3, t, t + rts), (4, t + rts, t + rts + SIFS),
             (5, t + rts + SIFS, t + rts + SIFS + cts), (4, p0 - SIFS, p0), (6, p0, p0 + LEGACY_PHY), (7, p0 + LEGACY_PHY, d0),
             (8, d0, d1), (4, d1, d1 + SIFS), (9, d1 + SIFS, d1 + SIFS + ba)]
    for n, u0, u1 in parts:
        cx = ((x_of(u0, lay) + x_of(u1, lay)) / 2 - xa) * scale
        d.line((cx, band / 2 + MARK_R, cx, band + 4), fill=(255, 255, 255), width=2)
        marker(d, cx, band / 2, n)
    save(fig, out / "txop.png")
    return {"slots": a["slots"], "drawn": a["drawn"], "mpdus": x["mpdus"], "mcs": x["mcs"], "sta": x["sta"],
            "ac": x["ac"], "data_us": round((d1 - d0) * 1e6), "t": t}


def fig_collision(binary, work, ev, c, out):
    span = 0.45e-3
    img, lay = shot(binary, work, "collision", (c["t"] + c["end"]) / 2, span, cursor=c["t"] - 0.4 * span)
    lanes = lay["lanes"]
    top, bottom = lanes[0]["y"] - 4, lanes[-1]["y"] + lanes[-1]["h"] + 4
    crop = img.crop((0, top, lay["panel_x"] + lay["panel_w"] + 4, bottom))
    band = 34
    fig = with_band(crop, 1, band)
    d = ImageDraw.Draw(fig)
    cx = x_of((c["t"] + c["end"]) / 2, lay)
    lc = lane(lay, c["link"])
    d.line((cx, band / 2 + MARK_R, cx, band + lc["y"] - top + 4), fill=(255, 255, 255), width=2)
    marker(d, cx, band / 2, 5)
    save(fig, out / "collision.png")


def fig_mlo(binary, work, ev, e, out):
    span = max(3 * (e["t1"] - e["t0"]), 0.3e-3)
    mid = (e["t0"] + e["t1"]) / 2
    img, lay = shot(binary, work, "mlo", mid, span)
    lanes = lay["lanes"]
    top, bottom = lanes[0]["y"] - 4, lanes[-1]["y"] + lanes[-1]["h"] + 4
    crop = img.crop((0, top, lay["panel_x"] + lay["panel_w"] + 4, bottom))
    band = 34
    fig = with_band(crop, 1, band)
    d = ImageDraw.Draw(fig)
    # the two TXOPs on air at once (the example spans both): a marker on each
    for link in e["lanes"]:
        on = [x for x in ev["txops"] if x["link"] == link and x["t"] < e["t1"] and x["end"] > e["t0"]]
        x = max(on, key=lambda x: min(x["end"], e["t1"]) - max(x["t"], e["t0"]))
        ln = lane(lay, link)
        marker(d, x_of((x["t"] + x["end"]) / 2, lay), band + ln["y"] - top + ln["h"] / 2, 3)
    save(fig, out / "mlo.png")


def fig_backoff(binary, work, ev, e, out):
    span = max(1.3 * (e["t1"] - e["t0"]), 40e-6)
    anim = e["anim"]
    # the cursor in the countdown's second slot (anim: twice in AIFS, once per slot, then on the TXOP)
    slots = [t for t in anim[2:] if t < anim[-1] - 1e-6]
    cursor = slots[1] if len(slots) >= 2 else (e["t0"] + e["t1"]) / 2
    t_txop = anim[-1] - 2e-6 if anim else e["t1"]
    img, lay = shot(binary, work, "backoff", (e["t0"] + e["t1"]) / 2, span, cursor=cursor)
    reg = lay["regions"]
    lanes = lay["lanes"]
    top = lanes[0]["y"] - 4
    cw = reg["cw"]
    crop = img.crop((0, top, cw[0] + cw[2], cw[1] + cw[3]))
    d = ImageDraw.Draw(crop)
    ln = lane(lay, e["lanes"][0])
    marker(d, x_of((e["t0"] + t_txop) / 2, lay), ln["y"] - top + ln["h"] - 12, 4)
    marker(d, x_of(cursor, lay) + 14, lanes[-1]["y"] + lanes[-1]["h"] - top + 10, 6)
    marker(d, cw[0] + cw[2] - 18, cw[1] - top + 12, 7)
    save(crop, out / "backoff.png")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    target = os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target"))
    ap.add_argument("--bin", default=os.path.join(target, "release/examples/XR_sim"))
    ap.add_argument("--out", default=str(ROOT / "assets/viz_readme"))
    args = ap.parse_args()
    binary = Path(args.bin).resolve()
    if not binary.exists():
        sys.exit(f"no XR_sim at {binary}: cargo build --release --example XR_sim (in asynchronix/)")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as work:
        events = os.path.join(work, "events.json")
        run_viewer(binary, {"XRSIM_VIZ_EVENTS": events}, work)
        with open(events) as f:
            ev = json.load(f)
        txop, coll = pick_txop(ev), pick_collision(ev)
        fig_overview(binary, work, ev, coll, out)
        info = fig_txop(binary, work, ev, txop, out)
        fig_collision(binary, work, ev, coll, out)
        fig_mlo(binary, work, ev, example(ev, "Mlo"), out)
        fig_backoff(binary, work, ev, example(ev, "Backoff"), out)
    print("TXOP shown:", json.dumps(info))
    print("collision shown:", json.dumps({"t": coll["t"], "link": coll["link"], "contenders": coll["contenders"]}))


if __name__ == "__main__":
    main()
