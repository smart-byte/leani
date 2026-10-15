//! The agent skill's guide, compiled into the binary so it always matches
//! this release.

use std::io::{self, Write};

use anyhow::{Context, Result};

use crate::{cli::GuideTopic, process::Exit};

const OVERVIEW: &str = include_str!("../../../skills/leani/SKILL.md");
const PROCESSORS: &str = include_str!("../../../skills/leani/references/processors.md");

pub(crate) fn print(topic: Option<GuideTopic>) -> Result<Exit> {
    let mut stdout = io::stdout().lock();
    match stdout
        .write_all(text(topic).as_bytes())
        .and_then(|()| stdout.flush())
    {
        Err(error) if error.kind() != io::ErrorKind::BrokenPipe => {
            Err(error).context("write guide")
        }
        _ => Ok(Exit::Success),
    }
}

fn text(topic: Option<GuideTopic>) -> &'static str {
    match topic {
        None => without_front_matter(OVERVIEW),
        Some(GuideTopic::Processors) => PROCESSORS,
    }
}

/// The skill's YAML front matter addresses skill loaders, not readers.
fn without_front_matter(markdown: &str) -> &str {
    markdown
        .strip_prefix("---\n")
        .and_then(|rest| rest.split_once("\n---\n"))
        .map_or(markdown, |(_, body)| body.trim_start())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topics_print_the_embedded_skill() {
        let overview = text(None);
        assert!(
            overview.starts_with("# Leani\n"),
            "front matter left in: {overview:.80}"
        );
        assert!(overview.contains("`leani guide processors`"));
        assert!(text(Some(GuideTopic::Processors)).contains("## 4. Write a custom Rust processor"));
    }
}
