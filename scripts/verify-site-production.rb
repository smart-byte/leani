#!/usr/bin/env ruby
# frozen_string_literal: true

require "open3"
require "pathname"

root = Pathname.new(__dir__).join("..").realpath
ref = ARGV.fetch(0, "HEAD")

def git(root, *arguments)
  output, status = Open3.capture2e("git", "-C", root.to_s, *arguments)
  abort output unless status.success?
  output.strip
end

commit = git(root, "rev-parse", "#{ref}^{commit}")
tags = git(root, "tag", "--points-at", commit).lines.map(&:strip).grep(/\Av\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?\z/)
abort "#{commit} must carry exactly one Leani release tag" unless tags.length == 1

version = root.join("Cargo.toml").read[/^version\s*=\s*"([^"]+)"$/, 1]
abort "could not read workspace package version" unless version
abort "release tag #{tags.first} does not match workspace version v#{version}" unless tags.first == "v#{version}"

puts "site production ref #{commit} matches #{tags.first}"
