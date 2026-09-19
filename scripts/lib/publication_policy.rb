# frozen_string_literal: true

module PublicationPolicy
  PRIVATE_PATHS = [
    %r{(?:\A|/)(?:\.private|\.agents|\.codex|\.claude|\.ssh|\.aws|\.gnupg|\.planning|\.specify)(?:/|\z)}i,
    %r{(?:\A|/)(?:plans?|planning|superpowers|design-explorations|reports|evidence)(?:/|\z)}i,
    %r{(?:\A|/)(?:AGENTS|CLAUDE|BACKLOG|ROADMAP|PLAN)(?:\.md)?\z}i,
    %r{(?:\A|/)[^/]*(?:-PLAN|-READINESS|-CHECKLIST|IMPLEMENTATION-DESIGN|IMPLEMENTATION-STATUS|XATU-OPTIMIZATIONS)\.md\z}i,
    %r{(?:\A|/)\.env(?:\..*)?\z}i,
    %r{(?:\A|/)(?:id_rsa|id_ed25519|credentials|credentials\.json)\z}i,
    %r{\A(?:benchmark[^/]*\.(?:json|ndjson)|benchmark-sweep-results)(?:/|\z)}i,
  ].freeze

  PRIVATE_REFS = %r{\Arefs/(?:heads/(?:archive|backup|private)/|original/|replace/)}i

  CONTENT_RULES = {
    "local user directory" => %r{/(?:Users|home)/[^/\s"<>]+/|[A-Za-z]:[\\/]+Users[\\/]+[^\\/\s]+[\\/]}n,
    "machine-specific temporary directory" => %r{/(?:private/)?var/folders/[^\s"<>]+}n,
    "private key" => /-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----/n,
    "provider credential" => /\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{40,}|AKIA[0-9A-Z]{16}|npm_[A-Za-z0-9]{30,}|xox[baprs]-[A-Za-z0-9-]{15,})\b/n,
    "credential in URL" => %r{https?://[^\s/"<>:]+:[^\s/"<>@]+@[^\s/"<>]+}n,
    "agent implementation plan" => /(?:REQUIRED SUB[-]SKILL:|For (?:agentic workers|Claude):)/n,
  }.freeze

  def self.private_path?(path)
    PRIVATE_PATHS.any? { |pattern| pattern.match?(path) }
  end

  def self.content_findings(content)
    content = content.b
    CONTENT_RULES.filter_map do |label, pattern|
      match = pattern.match(content)
      [label, content.byteslice(0, match.begin(0)).count("\n") + 1] if match
    end
  end
end
