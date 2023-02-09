use journal::EntryType;

const CUSTOM_MESSAGE_MASK: u16 = 0xFC00;
const COMPLETED_MASK: u64 = 0x0001_0000_0000;
const VERSION_MASK: u64 = 0x03FF_0000_0000;

type MessageTypeId = u16;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MessageKind {
    Core,
    IO,
    State,
    Syscall,
    Custom,
}

// This macro generates:
// * MessageType enum
// * MessageType#kind(&self) -> MessageKind
// * From<MessageType> for MessageTypeId (used for serializing message header)
// * TryFrom<MessageTypeId> for MessageType (used for deserializing message header)
// * From<EntryType> for MessageType (used to convert from journal entry to message header)
// * TryFrom<MessageType> for EntryType (used to convert from message header to journal entry)
macro_rules! gen_message_type_enum {
    (@gen_enum [] -> [$($body:tt)*]) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq)]
        pub enum MessageType {
            $($body)*
            Custom(u16)
        }
    };
    (@gen_enum [$variant:ident Entry $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message_type_enum!(@gen_enum [$($tail)*] -> [[<$variant Entry>], $($body)*]); }
    };
    (@gen_enum [$variant:ident $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_enum [$($tail)*] -> [$variant, $($body)*]);
    };

    (@gen_kind_impl [] -> [$($variant:ident, $kind:ident,)*]) => {
        impl MessageType {
            pub fn kind(&self) -> MessageKind {
                match self {
                    $(MessageType::$variant => MessageKind::$kind,)*
                    MessageType::Custom(_) => MessageKind::Custom
                }
            }
        }
    };
    (@gen_kind_impl [$variant:ident Entry $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message_type_enum!(@gen_kind_impl [$($tail)*] -> [[<$variant Entry>], $kind, $($body)*]); }
    };
    (@gen_kind_impl [$variant:ident $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_kind_impl [$($tail)*] -> [$variant, $kind, $($body)*]);
    };

    (@gen_to_id [] -> [$($variant:ident, $id:literal,)*]) => {
        impl From<MessageType> for MessageTypeId {
            fn from(mt: MessageType) -> Self {
                match mt {
                    $(MessageType::$variant => $id,)*
                    MessageType::Custom(id) => id
                }
            }
        }
    };
    (@gen_to_id [$variant:ident Entry $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message_type_enum!(@gen_to_id [$($tail)*] -> [[<$variant Entry>], $id, $($body)*]); }
    };
    (@gen_to_id [$variant:ident $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_to_id [$($tail)*] -> [$variant, $id, $($body)*]);
    };

    (@gen_from_id [] -> [$($variant:ident, $id:literal,)*]) => {
        impl TryFrom<MessageTypeId> for MessageType {
            type Error = UnknownMessageType;

            fn try_from(value: MessageTypeId) -> Result<Self, Self::Error> {
                match value {
                    $($id => Ok(MessageType::$variant),)*
                    v if ((v & CUSTOM_MESSAGE_MASK) != 0) => Ok(MessageType::Custom(v)),
                    v => Err(UnknownMessageType(v))
                }
            }
        }
    };
    (@gen_from_id [$variant:ident Entry $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message_type_enum!(@gen_from_id [$($tail)*] -> [[<$variant Entry>], $id, $($body)*]); }
    };
    (@gen_from_id [$variant:ident $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_from_id [$($tail)*] -> [$variant, $id, $($body)*]);
    };

    (@gen_to_entry_type [] -> [$($variant:ident, $res:expr,)*]) => {
        impl TryFrom<MessageType> for EntryType {
            type Error = &'static str;

             fn try_from(mt: MessageType) -> Result<Self, Self::Error> {
                match mt {
                    $(MessageType::$variant => $res,)*
                    MessageType::Custom(id) => Ok(EntryType::Custom(id))
                }
             }
        }
    };
    (@gen_to_entry_type [$variant:ident Entry $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        paste::paste! { gen_message_type_enum!(@gen_to_entry_type [$($tail)*] -> [[<$variant Entry>], Ok(EntryType::$variant), $($body)*]); }
    };
    (@gen_to_entry_type [$variant:ident $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_to_entry_type [$($tail)*] -> [$variant, Err(concat!(stringify!($variant), " is not an entry message")), $($body)*]);
    };

    (@gen_from_entry_type [] -> [$($variant:ident,)*]) => {
        impl From<EntryType> for MessageType {
             fn from(et: EntryType) -> Self {
                match et {
                    $(EntryType::$variant => paste::paste! { MessageType::[<$variant Entry >] },)*
                    EntryType::Custom(id) => MessageType::Custom(id)
                }
             }
        }
    };
    (@gen_from_entry_type [$variant:ident Entry $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_from_entry_type [$($tail)*] -> [$variant, $($body)*]);
    };
    (@gen_from_entry_type [$variant:ident $kind:ident = $id:literal; $($tail:tt)*] -> [$($body:tt)*]) => {
        gen_message_type_enum!(@gen_from_entry_type [$($tail)*] -> [$($body)*]);
    };

    // Entrypoint of the macro
    ($($tokens:tt)*) => {
        gen_message_type_enum!(@gen_enum [$($tokens)*] -> []);
        gen_message_type_enum!(@gen_kind_impl [$($tokens)*] -> []);
        gen_message_type_enum!(@gen_to_id [$($tokens)*] -> []);
        gen_message_type_enum!(@gen_from_id [$($tokens)*] -> []);
        gen_message_type_enum!(@gen_to_entry_type [$($tokens)*] -> []);
        gen_message_type_enum!(@gen_from_entry_type [$($tokens)*] -> []);
    };
}

gen_message_type_enum!(
    Start Core = 0x0000;
    Completion Core = 0x0001;
    PollInputStream Entry IO = 0x0400;
    OutputStream Entry IO = 0x0401;
    GetState Entry State = 0x0800;
    SetState Entry State = 0x0801;
    ClearState Entry State = 0x0802;
    Sleep Entry Syscall = 0x0C00;
    Invoke Entry Syscall = 0x0C01;
    BackgroundInvoke Entry Syscall = 0x0C02;
    Awakeable Entry Syscall = 0x0C03;
    CompleteAwakeable Entry Syscall = 0x0C04;
);

impl MessageType {
    fn allows_completed_flag(&self) -> bool {
        matches!(
            self,
            MessageType::PollInputStreamEntry
                | MessageType::GetStateEntry
                | MessageType::SleepEntry
                | MessageType::InvokeEntry
                | MessageType::AwakeableEntry
        )
    }

    fn allows_protocol_version(&self) -> bool {
        *self == MessageType::Start
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessageHeader {
    ty: MessageType,
    length: u32,

    // Flags
    /// Only `CompletableEntries` allow completed flag. See [`MessageType#allows_completed_flag`].
    completed_flag: Option<bool>,
    /// Only `StartMessage` allows protocol_version.
    protocol_version: Option<u16>,
}

impl MessageHeader {
    #[inline]
    pub fn new(ty: MessageType, length: u32) -> Self {
        Self::_new(ty, None, None, length)
    }

    #[inline]
    pub fn new_start(protocol_version: u16, length: u32) -> Self {
        Self::_new(MessageType::Start, None, Some(protocol_version), length)
    }

    #[inline]
    pub fn new_completable_entry(ty: MessageType, completed: bool, length: u32) -> Self {
        debug_assert!(ty.allows_completed_flag());

        Self::_new(ty, Some(completed), None, length)
    }

    #[inline]
    fn _new(
        ty: MessageType,
        completed: Option<bool>,
        protocol_version: Option<u16>,
        length: u32,
    ) -> Self {
        MessageHeader {
            ty,
            length,
            completed_flag: completed,
            protocol_version,
        }
    }

    #[inline]
    pub fn message_kind(&self) -> MessageKind {
        self.ty.kind()
    }

    #[inline]
    pub fn message_type(&self) -> MessageType {
        self.ty
    }

    #[inline]
    pub fn completed(&self) -> Option<bool> {
        self.completed_flag
    }

    #[inline]
    pub fn protocol_version(&self) -> Option<u16> {
        self.protocol_version
    }

    #[inline]
    pub fn frame_length(&self) -> u32 {
        self.length
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown message code {0:#x}")]
pub struct UnknownMessageType(u16);

impl TryFrom<u64> for MessageHeader {
    type Error = UnknownMessageType;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        let ty_code = (value >> 48) as u16;
        let ty: MessageType = ty_code.try_into()?;
        let completed = if ty.allows_completed_flag() {
            Some((value & COMPLETED_MASK) != 0)
        } else {
            None
        };
        let protocol_version = if ty.allows_protocol_version() {
            Some(((value & VERSION_MASK) >> 32) as u16)
        } else {
            None
        };
        let length = value as u32;

        Ok(MessageHeader::_new(ty, completed, protocol_version, length))
    }
}

impl From<MessageHeader> for u64 {
    fn from(message_header: MessageHeader) -> Self {
        let mut res =
            ((u16::from(message_header.ty) as u64) << 48) | (message_header.length as u64);

        if let Some(true) = message_header.completed_flag {
            res |= COMPLETED_MASK;
        }
        if let Some(protocol_version) = message_header.protocol_version {
            res |= (protocol_version as u64) << 32;
        }

        res
    }
}

#[cfg(test)]
mod tests {

    use super::{MessageKind::*, MessageType::*, *};

    macro_rules! roundtrip_test {
        ($test_name:ident, $header:expr, $ty:expr, $kind:expr, $done:expr, $protocol_version:expr, $len:expr) => {
            #[test]
            fn $test_name() {
                let serialized: u64 = $header.into();
                let header: MessageHeader = serialized.try_into().unwrap();

                assert_eq!(header.message_type(), $ty);
                assert_eq!(header.message_kind(), $kind);
                assert_eq!(header.completed(), $done);
                assert_eq!(header.protocol_version(), $protocol_version);
                assert_eq!(header.frame_length(), $len);
            }
        };
    }

    roundtrip_test!(
        invoke_test,
        MessageHeader::new_start(1, 25),
        Start,
        Core,
        None,
        Some(1),
        25
    );

    roundtrip_test!(
        completion_test,
        MessageHeader::new(Completion, 22),
        Completion,
        Core,
        None,
        None,
        22
    );

    roundtrip_test!(
        completed_get_state,
        MessageHeader::new_completable_entry(GetStateEntry, true, 0),
        GetStateEntry,
        State,
        Some(true),
        None,
        0
    );

    roundtrip_test!(
        not_completed_get_state,
        MessageHeader::new_completable_entry(GetStateEntry, false, 0),
        GetStateEntry,
        State,
        Some(false),
        None,
        0
    );

    roundtrip_test!(
        completed_get_state_with_len,
        MessageHeader::new_completable_entry(GetStateEntry, true, 10341),
        GetStateEntry,
        State,
        Some(true),
        None,
        10341
    );

    roundtrip_test!(
        custom_entry,
        MessageHeader::new(MessageType::Custom(0xFC00), 10341),
        MessageType::Custom(0xFC00),
        MessageKind::Custom,
        None,
        None,
        10341
    );
}
