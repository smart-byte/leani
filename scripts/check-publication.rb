#!/usr/bin/env ruby
# frozen_string_literal: true

require "open3"
require_relative "lib/publication_policy"

# Use the current repository, including linked worktrees and disposable tests.
# Never print matched content: it may contain a secret.
repository, repository_status = Open3.capture2e("git", "rev-parse", "--show-toplevel")
abort "not a Git working tree; publication check cannot continue" unless repository_status.success?
ROOT = repository.strip.freeze

def git(*arguments)
  output, status = Open3.capture2e("git", "-C", ROOT, *arguments)
  abort "git #{arguments.first} failed; publication check cannot continue" unless status.success?
  output
end

def check_entries(entries, label, cache, errors)
  entries.each do |path, oid|
    errors << "#{label}: private working file #{path}" if PublicationPolicy.private_path?(path)
    next unless oid
    findings = cache[oid] ||= PublicationPolicy.content_findings(git("cat-file", "blob", oid))
    findings.each { |kind, line| errors << "#{label}: #{kind} in #{path}:#{line}" }
  end
end

def tree_entries(ref)
  git("ls-tree", "-r", "-z", ref).split("\0").map do |entry|
    metadata, path = entry.split("\t", 2)
    _mode, type, oid = metadata.split
    [path, type == "blob" ? oid : nil]
  end
end

errors = []
cache = {}
mode = ARGV.first
case mode
when "--staged"
  abort "usage: check-publication.rb --staged" unless ARGV.length == 1
  entries = git("ls-files", "--stage", "-z").split("\0").map do |entry|
    metadata, path = entry.split("\t", 2)
    _mode, oid, stage = metadata.split
    abort "unmerged index; publication check cannot continue" unless stage == "0"
    [path, oid]
  end
  check_entries(entries, "index", cache, errors)
  summary = "staged tree"
when "--message"
  abort "usage: check-publication.rb --message FILE" unless ARGV.length == 2
  PublicationPolicy.content_findings(File.binread(ARGV[1])).each do |kind, line|
    errors << "commit message: #{kind} at line #{line}"
  end
  summary = "commit message"
else
  refs = if mode == "--pre-push"
           abort "usage: check-publication.rb --pre-push" unless ARGV.length == 1
           $stdin.each_line.filter_map do |line|
             local_ref, local_oid, remote_ref, remote_oid = line.split
             unless [local_oid, remote_oid].all? { |oid| oid&.match?(/\A[0-9a-f]{40,64}\z/) } && remote_ref
               abort "invalid pre-push input; publication check cannot continue"
             end
             next if local_oid.match?(/\A0+\z/)
             if [local_ref, remote_ref].any? { |ref| PublicationPolicy::PRIVATE_REFS.match?(ref) }
               errors << "private or recovery ref cannot be pushed: #{local_ref} -> #{remote_ref}"
             end
             local_oid
           end
         else
           abort "unknown publication check option" if ARGV.any? { |ref| ref.start_with?("-") }
           ARGV.empty? ? ["HEAD"] : ARGV
         end

  commits = refs.flat_map do |ref|
    object = ref
    while git("cat-file", "-t", object).strip == "tag"
      tag = git("cat-file", "tag", object)
      PublicationPolicy.content_findings(tag).each do |kind, line|
        errors << "#{ref}: #{kind} in tag at line #{line}"
      end
      object = tag.lines.first.split.last
    end
    commit = git("rev-parse", "--verify", "#{ref}^{commit}").strip
    # Performance checks apply to the proposed tip. The published base once
    # contained an older workflow that has since been removed.
    tree_entries(commit).each do |path, oid|
      next unless oid && path.start_with?(".github/workflows/")
      if /\bbenchmark(?:s|ing)?\b/i.match?(git("cat-file", "blob", oid))
        errors << "#{commit}: performance evidence must stay local: #{path}"
      end
    end
    git("rev-list", commit).lines.map(&:strip)
  end.uniq

  commits.each do |commit|
    check_entries(tree_entries(commit), commit, cache, errors)
    PublicationPolicy.content_findings(git("cat-file", "commit", commit)).each do |kind, line|
      errors << "#{commit}: #{kind} in commit metadata/message at line #{line}"
    end
  end
  summary = "#{commits.length} reachable commits"
end

if errors.empty?
  puts "publication check passed for #{summary}"
else
  warn errors.uniq.join("\n")
  warn "Keep private material under ignored .private/; remove it from every unpublished commit before pushing."
  exit 1
end
