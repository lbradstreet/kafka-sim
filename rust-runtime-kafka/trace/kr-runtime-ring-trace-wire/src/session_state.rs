#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum SessionState {
    Absent = 0x0_u8,
    Open = 0x1_u8,
    Closed = 0x2_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for SessionState {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::Absent,
            0x1_u8 => Self::Open,
            0x2_u8 => Self::Closed,
            _ => Self::NullVal,
        }
    }
}
impl From<SessionState> for u8 {
    #[inline]
    fn from(v: SessionState) -> Self {
        match v {
            SessionState::Absent => 0x0_u8,
            SessionState::Open => 0x1_u8,
            SessionState::Closed => 0x2_u8,
            SessionState::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for SessionState {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Absent" => Ok(Self::Absent),
            "Open" => Ok(Self::Open),
            "Closed" => Ok(Self::Closed),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for SessionState {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Absent => write!(f, "Absent"),
            Self::Open => write!(f, "Open"),
            Self::Closed => write!(f, "Closed"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
