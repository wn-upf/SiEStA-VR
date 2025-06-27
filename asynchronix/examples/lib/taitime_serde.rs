// src/taitime_serde.rs
use serde::{Deserializer, Serializer};
use serde::de::Error as DeError;
use serde::{Deserialize};
use tai_time::TaiTime;
use crate::format_elapsed;

pub fn serialize<S>(t: &TaiTime<0>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // turn your TaiTime into a Duration since your chosen epoch…
    let elapsed: std::time::Duration = t.duration_since(TaiTime::EPOCH);
    let secs = format_elapsed!(elapsed);
    s.serialize_str(&secs)
}

pub fn deserialize<'de, D>(d: D) -> Result<TaiTime<0>, D::Error>
where
    D: Deserializer<'de>,
{
    // 1) read the string
    let s = String::deserialize(d)?;
    // 2) parse into float seconds
    let total = s.parse::<f64>().map_err(DeError::custom)?;
    // 3) split to integer secs + fractional nsec
    let secs_i = total.trunc() as i64;
    let frac = total.fract();
    let nsec = (frac * 1_000_000_000.0).round() as u32;
    // 4) build a TaiTime<0> with the `new` constructor
    TaiTime::<0>::new(secs_i, nsec)
        .ok_or_else(|| DeError::custom(format!("invalid TaiTime values {}s + {}ns", secs_i, nsec)))
}
