#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum StorageOutcome {
    Success = 0x0_u8,
    Rejected = 0x1_u8,
    Failed = 0x2_u8,
    Crash = 0x3_u8,
    Recovered = 0x4_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for StorageOutcome {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::Success,
            0x1_u8 => Self::Rejected,
            0x2_u8 => Self::Failed,
            0x3_u8 => Self::Crash,
            0x4_u8 => Self::Recovered,
            _ => Self::NullVal,
        }
    }
}
impl From<StorageOutcome> for u8 {
    #[inline]
    fn from(v: StorageOutcome) -> Self {
        match v {
            StorageOutcome::Success => 0x0_u8,
            StorageOutcome::Rejected => 0x1_u8,
            StorageOutcome::Failed => 0x2_u8,
            StorageOutcome::Crash => 0x3_u8,
            StorageOutcome::Recovered => 0x4_u8,
            StorageOutcome::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for StorageOutcome {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Success" => Ok(Self::Success),
            "Rejected" => Ok(Self::Rejected),
            "Failed" => Ok(Self::Failed),
            "Crash" => Ok(Self::Crash),
            "Recovered" => Ok(Self::Recovered),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for StorageOutcome {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Success => write!(f, "Success"),
            Self::Rejected => write!(f, "Rejected"),
            Self::Failed => write!(f, "Failed"),
            Self::Crash => write!(f, "Crash"),
            Self::Recovered => write!(f, "Recovered"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
