//! Incremental `text/event-stream` parsing, as the HTML event-stream
//! interpretation specifies: CR, LF, or CRLF ends a line, the last `event`
//! field names the event, a value loses only one leading space, and an event
//! without data is not dispatched.

use anyhow::{Context, Result, bail};
use bytes::{Buf as _, BytesMut};

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const BYTE_ORDER_MARK: &[u8] = b"\xEF\xBB\xBF";

#[derive(Debug)]
pub(super) struct Event {
    /// The last `event` field; `None` is the default `message` type.
    pub(super) name: Option<String>,
    pub(super) data: String,
}

#[derive(Debug, Default)]
pub(super) struct EventBuffer {
    /// Received bytes not yet consumed as complete lines.
    bytes: BytesMut,
    /// Leading bytes of `bytes` already searched for a line ending, so a
    /// line arriving in small chunks is scanned once.
    scanned: usize,
    /// The last line ended with a CR at the end of the received bytes; a LF
    /// arriving next completes that CRLF.
    pending_lf: bool,
    /// Whether the start of the stream, which may carry a byte-order mark,
    /// was handled.
    started: bool,
    /// Bytes of the event being assembled, bounded by `MAX_FRAME_BYTES`.
    event_bytes: usize,
    event_type: String,
    data: String,
    /// Bytes searched for a line ending so far, for the single-scan test.
    #[cfg(test)]
    examined: usize,
}

impl EventBuffer {
    pub(super) fn push(&mut self, chunk: &[u8]) {
        self.bytes.extend_from_slice(chunk);
    }

    /// The next complete event, or `None` until more bytes arrive.
    pub(super) fn next_event(&mut self) -> Result<Option<Event>> {
        while let Some(line) = self.next_line()? {
            if line.is_empty() {
                if let Some(event) = self.dispatch() {
                    return Ok(Some(event));
                }
            } else {
                self.field(&line)?;
            }
        }
        Ok(None)
    }

    fn next_line(&mut self) -> Result<Option<BytesMut>> {
        if !self.started {
            if self.bytes.len() < BYTE_ORDER_MARK.len() && BYTE_ORDER_MARK.starts_with(&self.bytes)
            {
                // Too few bytes yet to tell a byte-order mark apart.
                return Ok(None);
            }
            if self.bytes.starts_with(BYTE_ORDER_MARK) {
                self.bytes.advance(BYTE_ORDER_MARK.len());
            }
            self.started = true;
        }
        if self.pending_lf && !self.bytes.is_empty() {
            self.pending_lf = false;
            if self.bytes.starts_with(b"\n") {
                self.bytes.advance(1);
            }
        }
        let found = self.bytes[self.scanned..]
            .iter()
            .position(|byte| matches!(byte, b'\r' | b'\n'));
        #[cfg(test)]
        {
            self.examined += found.map_or(self.bytes.len() - self.scanned, |offset| offset + 1);
        }
        let Some(offset) = found else {
            self.scanned = self.bytes.len();
            self.check_frame_size(self.bytes.len())?;
            return Ok(None);
        };
        let end = self.scanned + offset;
        self.check_frame_size(end)?;
        let line = self.bytes.split_to(end);
        let crlf = self.bytes.starts_with(b"\r\n");
        self.pending_lf = self.bytes.as_ref() == b"\r";
        self.bytes.advance(if crlf { 2 } else { 1 });
        self.scanned = 0;
        self.event_bytes = self.event_bytes.saturating_add(end + 1);
        Ok(Some(line))
    }

    fn check_frame_size(&self, partial_line: usize) -> Result<()> {
        if self.event_bytes.saturating_add(partial_line) > MAX_FRAME_BYTES {
            bail!("node SSE frame exceeded the {MAX_FRAME_BYTES} byte safety limit");
        }
        Ok(())
    }

    fn field(&mut self, line: &[u8]) -> Result<()> {
        let line = std::str::from_utf8(line).context("node SSE stream is not UTF-8")?;
        let (name, value) = line.split_once(':').map_or((line, ""), |(name, value)| {
            (name, value.strip_prefix(' ').unwrap_or(value))
        });
        match name {
            "event" => value.clone_into(&mut self.event_type),
            "data" => {
                self.data.push_str(value);
                self.data.push('\n');
            }
            // Comments (an empty name, as for heartbeats), `id`, `retry`, and
            // unknown fields do not affect this client: it resumes from each
            // change envelope's cursor.
            _ => {}
        }
        Ok(())
    }

    /// End the event at a blank line. One without data is dropped.
    fn dispatch(&mut self) -> Option<Event> {
        self.event_bytes = 0;
        let name = std::mem::take(&mut self.event_type);
        let mut data = std::mem::take(&mut self.data);
        if data.is_empty() {
            return None;
        }
        if data.ends_with('\n') {
            data.pop();
        }
        Some(Event {
            name: (!name.is_empty()).then_some(name),
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_standard_line_endings() {
        assert_eq!(
            events(&[b"event: apply\ndata: {\"sequence\":\"1\"}\n\nrest"]),
            [event(Some("apply"), "{\"sequence\":\"1\"}")]
        );
        assert_eq!(events(&[b"data: {}\r\n\r\n"]), [event(None, "{}")]);
    }

    #[test]
    fn events_without_data_are_not_dispatched() {
        // A heartbeat comment, or a type without data, dispatches nothing,
        // and its type does not leak into the next event.
        assert_eq!(
            events(&[b":heartbeat\n\nevent: hello\n\ndata: x\n\n"]),
            [event(None, "x")]
        );
    }

    /// The next dispatched event as `(name, data)`.
    fn next(buffer: &mut EventBuffer) -> Option<(Option<String>, String)> {
        buffer
            .next_event()
            .expect("read event")
            .map(|event| (event.name, event.data))
    }

    /// Every event dispatched while `chunks` arrive one at a time.
    fn events(chunks: &[&[u8]]) -> Vec<(Option<String>, String)> {
        let mut buffer = EventBuffer::default();
        let mut events = Vec::new();
        for chunk in chunks {
            buffer.push(chunk);
            while let Some(event) = next(&mut buffer) {
                events.push(event);
            }
        }
        events
    }

    fn event(name: Option<&str>, data: &str) -> (Option<String>, String) {
        (name.map(str::to_owned), data.to_owned())
    }

    #[test]
    fn a_lone_carriage_return_ends_a_line() {
        // Audit SSE-2: only LF and CRLF ended lines.
        assert_eq!(
            events(&[b"event: hello\rdata: {}\r\r"]),
            [event(Some("hello"), "{}")]
        );
        // A CRLF split across chunks is one line ending, not two.
        assert_eq!(
            events(&[b"data: a\r", b"\ndata: b\r", b"\n\r", b"\n"]),
            [event(None, "a\nb")]
        );
    }

    #[test]
    fn the_last_event_field_names_the_event() {
        // Audit SSE-3: the first `event:` value won.
        assert_eq!(
            events(&[b"event: apply\nevent: hello\ndata: {}\n\n"]),
            [event(Some("hello"), "{}")]
        );
    }

    #[test]
    fn a_field_value_loses_only_one_leading_space() {
        // Audit SSE-4: every leading space was trimmed. A field without a
        // colon has an empty value.
        assert_eq!(
            events(&[b"data:  two\ndata:none\ndata\n\n"]),
            [event(None, " two\nnone\n")]
        );
    }

    #[test]
    fn a_byte_order_mark_does_not_hide_the_first_field() {
        assert_eq!(
            events(&[b"\xEF\xBB", b"\xBFdata: x\n\n"]),
            [event(None, "x")]
        );
    }

    #[test]
    fn a_frame_arriving_byte_by_byte_is_scanned_once() {
        // Audit SSE-5: every chunk rescanned the whole buffer, which is
        // quadratic in the frame size.
        const BYTES: usize = 512 * 1_024;
        let mut buffer = EventBuffer::default();
        buffer.push(b"data: ");
        for _ in 0..BYTES {
            buffer.push(b"x");
            assert!(next(&mut buffer).is_none());
        }
        buffer.push(b"\n\n");
        assert_eq!(next(&mut buffer).map(|(_, data)| data.len()), Some(BYTES));
        // Each received byte is searched for a line ending at most once.
        let received = b"data: ".len() + BYTES + 2;
        assert!(
            buffer.examined <= received,
            "{} bytes examined for {received} received",
            buffer.examined
        );
    }

    #[test]
    fn rejects_oversized_incomplete_frames() {
        let mut buffer = EventBuffer::default();
        buffer.push(&vec![b'x'; MAX_FRAME_BYTES + 1]);
        let error = buffer
            .next_event()
            .expect_err("oversized frame must be rejected");
        assert!(error.to_string().contains("SSE frame exceeded"));

        // The bound covers the whole event, not only its current line.
        let line = [
            b"data: ".as_slice(),
            &vec![b'x'; MAX_FRAME_BYTES / 2],
            b"\n",
        ]
        .concat();
        let mut buffer = EventBuffer::default();
        buffer.push(&line);
        buffer.push(&line);
        let error = buffer
            .next_event()
            .expect_err("oversized frame must be rejected");
        assert!(error.to_string().contains("SSE frame exceeded"));
    }
}
