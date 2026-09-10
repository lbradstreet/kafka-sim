#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum CompletionCertainty {
    NotApplicable = 0x0_u8,
    NotApplied = 0x1_u8,
    Applied = 0x2_u8,
    MayHaveApplied = 0x3_u8,
    #[default]
    NullVal = 0xff_u8,
}
impl From<u8> for CompletionCertainty {
    #[inline]
    fn from(v: u8) -> Self {
        match v {
            0x0_u8 => Self::NotApplicable,
            0x1_u8 => Self::NotApplied,
            0x2_u8 => Self::Applied,
            0x3_u8 => Self::MayHaveApplied,
            _ => Self::NullVal,
        }
    }
}
impl From<CompletionCertainty> for u8 {
    #[inline]
    fn from(v: CompletionCertainty) -> Self {
        match v {
            CompletionCertainty::NotApplicable => 0x0_u8,
            CompletionCertainty::NotApplied => 0x1_u8,
            CompletionCertainty::Applied => 0x2_u8,
            CompletionCertainty::MayHaveApplied => 0x3_u8,
            CompletionCertainty::NullVal => 0xff_u8,
        }
    }
}
impl core::str::FromStr for CompletionCertainty {
    type Err = ();

    #[inline]
    fn from_str(v: &str) -> core::result::Result<Self, Self::Err> {
        match v {
            "NotApplicable" => Ok(Self::NotApplicable),
            "NotApplied" => Ok(Self::NotApplied),
            "Applied" => Ok(Self::Applied),
            "MayHaveApplied" => Ok(Self::MayHaveApplied),
            _ => Ok(Self::NullVal),
        }
    }
}
impl core::fmt::Display for CompletionCertainty {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotApplicable => write!(f, "NotApplicable"),
            Self::NotApplied => write!(f, "NotApplied"),
            Self::Applied => write!(f, "Applied"),
            Self::MayHaveApplied => write!(f, "MayHaveApplied"),
            Self::NullVal => write!(f, "NullVal"),
        }
    }
}
