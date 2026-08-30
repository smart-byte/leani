#!/usr/bin/env ruby
# frozen_string_literal: true

require "open3"
require "pathname"

ROOT = Pathname.new(__dir__).join("..").realpath
FORBIDDEN_PATHS = [
  %r{\A\.private(?:/|\z)},
  %r{\AAGENTS\.md\z},
  %r{\Adocs/.*-PLAN\.md\z},
].freeze
LOCAL_PATH = %r{/(?:Users|home)/[^/\s]+/}.freeze
LOCAL_PATH_GREP = %r{/(Users|home)/[^/[:space:]]+/}.source.freeze
CI_BENCHMARK = /\bbenchmark(?:s|ing)?\b/i

def git(*arguments, allow_failure: false)
  output, status = Open3.capture2e("git", "-C", ROOT.to_s, *arguments)
  abort output unless status.success? || allow_failure
  [output, status.success?]
end

refs = ARGV.empty? ? ["HEAD"] : ARGV
commits = refs.flat_map do |ref|
  output, = git("rev-list", ref)
  output.lines.map(&:chomp)
end.uniq
abort "no commits resolved from #{refs.join(', ')}" if commits.empty?

errors = []
refs.each do |ref|
  workflows, = git("ls-tree", "-r", "--name-only", ref, "--", ".github/workflows")
  workflows.each_line(chomp: true) do |path|
    contents, = git("show", "#{ref}:#{path}")
    if CI_BENCHMARK.match?(contents)
      errors << "#{ref}: GitHub workflow #{path} contains benchmark automation; performance evidence is local-only"
    end
  end
end

commits.each do |commit|
  paths, = git("ls-tree", "-r", "--name-only", commit)
  paths.each_line(chomp: true) do |path|
    errors << "#{commit}: forbidden tracked path #{path}" if FORBIDDEN_PATHS.any? { |pattern| pattern.match?(path) }
  end

  matches, found = git("grep", "-n", "-I", "-E", LOCAL_PATH_GREP, commit, "--", ".", allow_failure: true)
  errors.concat(matches.lines.map { |line| "#{commit}: local absolute path: #{line.chomp}" }) if found

  message, = git("show", "-s", "--format=%B", commit)
  errors << "#{commit}: commit message contains a local absolute path" if LOCAL_PATH.match?(message)
end

tracked_private, = git("ls-files", "--", ".private")
errors << "current index tracks private paths: #{tracked_private.lines.map(&:chomp).join(', ')}" unless tracked_private.empty?

if errors.empty?
  puts "publication history is free of private paths across #{commits.length} commits"
else
  warn errors.join("\n")
  exit 1
end
