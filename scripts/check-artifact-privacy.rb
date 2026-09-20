#!/usr/bin/env ruby
# frozen_string_literal: true

require_relative "lib/publication_policy"

abort "usage: check-artifact-privacy.rb FILE_OR_DIRECTORY..." if ARGV.empty?
errors = []
count = 0
ARGV.each do |input|
  abort "artifact input does not exist" unless File.exist?(input)
  paths = File.directory?(input) ? Dir.glob(File.join(input, "**", "*"), File::FNM_DOTMATCH) : [input]
  paths.each do |path|
    next if File.directory?(path) && !File.symlink?(path)

    name = File.directory?(input) ? path.delete_prefix("#{input}/") : File.basename(path)
    if File.symlink?(path) || PublicationPolicy.private_path?(name)
      errors << "unexpected artifact path: #{name}"
      next
    end
    count += 1
    PublicationPolicy.content_findings(File.binread(path)).each do |kind, _line|
      errors << "#{name}: #{kind} (content redacted)"
    end
  end
end
abort "no artifact files found" if count.zero?
abort errors.uniq.join("\n") unless errors.empty?
puts "artifact privacy check passed for #{count} files"
