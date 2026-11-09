use discus_wire::{decode_frame, encode_frame, Frame, Limits, Message, TraceContext};

pub fn round_trip(message: Message) -> Message {
    let frame = Frame {
        request_id: 4,
        trace: TraceContext::default(),
        message,
    };
    decode_frame(&encode_frame(&frame), &Limits::default())
        .unwrap()
        .message
}
