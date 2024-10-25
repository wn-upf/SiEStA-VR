use colored::*;
use std::fmt::Display;

pub enum DebugColor {
    Red,
    Green,
    Blue,
}

impl DebugColor {
    fn to_color_fn(&self) -> fn(String) -> colored::ColoredString {
        match self {
            DebugColor::Red => |s| s.red(),
            DebugColor::Green => |s| s.green(),
            DebugColor::Blue => |s| s.blue(),
        }
    }
}

#[macro_export]
macro_rules! debug_print {
    ($color:expr, $fmt:expr, $($arg:tt)*) => {
        let msg = format!($fmt, $($arg)*);
        println!("{}", $color.to_color_fn()(msg));
    };
}

pub trait DebugPrint {
    fn print_debug(&self, color: DebugColor, prefix: &str);
}

impl DebugPrint for MpduPacket {
    fn print_debug(&self, color: DebugColor, prefix: &str) {
        debug_print!(
            color,
            "[{}] Packet ID: {}, Length: {}",
            prefix,
            self.packet_id,
            self.length_packet
        );
    }
}