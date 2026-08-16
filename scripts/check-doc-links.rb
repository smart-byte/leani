#!/usr/bin/env ruby
# frozen_string_literal: true

require "pathname"
require "uri"
require "open3"

root = Pathname.new(__dir__).join("..").realpath
tracked, status = Open3.capture2("git", "-C", root.to_s, "ls-files", "--", "*.md")
abort "could not enumerate tracked Markdown files" unless status.success?
files = tracked.lines.map(&:chomp)
errors = []

def documentation_route_exists?(root, path)
  return false unless path.start_with?("/docs")

  slug = path.delete_prefix("/docs").sub(%r{\A/+}, "").sub(%r{/+\z}, "")
  base = root.join("docs", slug)
  candidates = if slug.empty?
    [root.join("docs/index.md"), root.join("docs/index.mdx")]
  else
    [Pathname.new("#{base}.md"), Pathname.new("#{base}.mdx"), base.join("index.md"), base.join("index.mdx")]
  end
  candidates.any?(&:file?)
end

files.each do |relative|
  source = root.join(relative)
  source.each_line.with_index(1) do |line, line_number|
    if line.match?(%r{/(?:Users|home)/[^ )]+})
      errors << "#{relative}:#{line_number}: local absolute path"
    end
    line.scan(/!?(?:\[[^\]]*\])\(([^)]+)\)/) do |match|
      raw = match.first.strip
      raw = raw[1...-1] if raw.start_with?("<") && raw.end_with?(">")
      target = raw.split(/\s+["']/).first
      next if target.nil? || target.empty?
      next if target.start_with?("#", "http://", "https://", "mailto:", "data:")

      path = target.split(/[?#]/, 2).first
      next if path.nil? || path.empty?

      decoded = URI.decode_www_form_component(path)
      if decoded.start_with?("/docs")
        errors << "#{relative}:#{line_number}: missing #{target}" unless documentation_route_exists?(root, decoded)
        next
      end
      resolved = source.dirname.join(decoded).cleanpath
      errors << "#{relative}:#{line_number}: missing #{target}" unless resolved.exist?
    rescue ArgumentError
      errors << "#{relative}:#{line_number}: invalid link encoding #{target}"
    end
  end
end

if errors.empty?
  puts "tracked Markdown links are valid"
else
  warn errors.join("\n")
  exit 1
end
