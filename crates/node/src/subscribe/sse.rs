use anyhow::{Context, Result, bail};
use bytes::BytesMut;

const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub(super) struct Event {
    pub(super) name: Option<String>,
    pub(super) data: Option<String>,
}

#[derive(Debug, Default)]
pub(super) struct EventBuffer {
    bytes: BytesMut,
}

impl EventBuffer {
    pub(super) fn push(&mut self, chunk: &[u8]) {
        self.bytes.extend_from_slice(chunk);
    }

    pub(super) fn next_frame(&mut self) -> Result<Option<BytesMut>> {
        let Some(end) = event_end(&self.bytes) else {
            if self.bytes.len() > MAX_FRAME_BYTES {
                bail!("node SSE frame exceeded the {MAX_FRAME_BYTES} byte safety limit");
            }
            return Ok(None);
        };
        if end > MAX_FRAME_BYTES {
            bail!("node SSE frame exceeded the {MAX_FRAME_BYTES} byte safety limit");
        }

        let mut event = self.bytes.split_to(end);
        let delimiter = if event.ends_with(b"\r\n\r\n") { 4 } else { 2 };
        event.truncate(event.len().saturating_sub(delimiter));
        Ok(Some(event))
    }
}

fn event_end(buffer: &[u8]) -> Option<usize> {
    let crlf = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| (position, position + 4));
    let lf = buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|position| (position, position + 2));
    match (crlf, lf) {
        (Some(left), Some(right)) => Some(if left.0 <= right.0 { left.1 } else { right.1 }),
        (Some((_, end)), None) | (None, Some((_, end))) => Some(end),
        (None, None) => None,
    }
}

pub(super) fn parse(event: &[u8]) -> Result<Event> {
    let event = std::str::from_utf8(event).context("node SSE stream is not UTF-8")?;
    let name = event
        .lines()
        .find_map(|line| line.strip_prefix("event:"))
        .map(|value| value.trim_start().to_owned());
    let data = event
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>();
    Ok(Event {
        name,
        data: (!data.is_empty()).then(|| data.join("\n")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_line_endings() {
        let mut buffer = EventBuffer::default();
        buffer.push(b"event: apply\ndata: {\"sequence\":\"1\"}\n\nrest");
        let frame = buffer.next_frame().expect("read frame").expect("frame");
        let event = parse(&frame).expect("parse event");
        assert_eq!(event.name.as_deref(), Some("apply"));
        assert_eq!(event.data.as_deref(), Some("{\"sequence\":\"1\"}"));

        let mut buffer = EventBuffer::default();
        buffer.push(b"data: {}\r\n\r\n");
        let frame = buffer.next_frame().expect("read frame").expect("frame");
        assert_eq!(
            parse(&frame).expect("parse event").data.as_deref(),
            Some("{}")
        );
    }

    #[test]
    fn rejects_oversized_incomplete_frames() {
        let mut buffer = EventBuffer::default();
        buffer.push(&vec![b'x'; MAX_FRAME_BYTES + 1]);
        let error = buffer
            .next_frame()
            .expect_err("oversized frame must be rejected");
        assert!(error.to_string().contains("SSE frame exceeded"));
    }
}
