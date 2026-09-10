#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum StorageOperation {
    Open = 0x0_u8,
    ScriptFault = 0x1_u8,
    WriteAt = 0x2_u8,
    ReadAt = 0x3_u8,
    SetLen = 0x4_u8,
    Len = 0x5_u8,
    Sync = 0x6_u8,
    Crash = 0x7_u8,
    Reopen = 0x8_u8,
    Close = 0x9_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for StorageOperation {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::Open,
            0x1_u8 => Self::ScriptFault,
            0x2_u8 => Self::WriteAt,
            0x3_u8 => Self::ReadAt,
            0x4_u8 => Self::SetLen,
            0x5_u8 => Self::Len,
            0x6_u8 => Self::Sync,
            0x7_u8 => Self::Crash,
            0x8_u8 => Self::Reopen,
            0x9_u8 => Self::Close,
            _ => Self::NullVal,
        }
    }
}
impl From<StorageOperation> for u8 {
    #[inline]
    fn from(v: StorageOperation) -> Self {
        match v {
            StorageOperation::Open => 0x0_u8,
            StorageOperation::ScriptFault => 0x1_u8,
            StorageOperation::WriteAt => 0x2_u8,
            StorageOperation::ReadAt => 0x3_u8,
            StorageOperation::SetLen => 0x4_u8,
            StorageOperation::Len => 0x5_u8,
            StorageOperation::Sync => 0x6_u8,
            StorageOperation::Crash => 0x7_u8,
            StorageOperation::Reopen => 0x8_u8,
            StorageOperation::Close => 0x9_u8,
            StorageOperation::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for StorageOperation {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "Open" => Ok(Self::Open),
            "ScriptFault" => Ok(Self::ScriptFault),
            "WriteAt" => Ok(Self::WriteAt),
            "ReadAt" => Ok(Self::ReadAt),
            "SetLen" => Ok(Self::SetLen),
            "Len" => Ok(Self::Len),
            "Sync" => Ok(Self::Sync),
            "Crash" => Ok(Self::Crash),
            "Reopen" => Ok(Self::Reopen),
            "Close" => Ok(Self::Close),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for StorageOperation {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Open => write!(f, "Open"),
            Self::ScriptFault => write!(f, "ScriptFault"),
            Self::WriteAt => write!(f, "WriteAt"),
            Self::ReadAt => write!(f, "ReadAt"),
            Self::SetLen => write!(f, "SetLen"),
            Self::Len => write!(f, "Len"),
            Self::Sync => write!(f, "Sync"),
            Self::Crash => write!(f, "Crash"),
            Self::Reopen => write!(f, "Reopen"),
            Self::Close => write!(f, "Close"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
