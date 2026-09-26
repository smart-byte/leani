#!/usr/bin/env ruby
# frozen_string_literal: true

# Fails when a Linux binary needs a newer glibc than the documented floor.
require "open3"

abort "usage: check-glibc-floor.rb BINARY FLOOR" unless ARGV.length == 2
binary, floor = ARGV
abort "floor must be a glibc version such as 2.39" unless floor.match?(/\A\d+\.\d+\z/)

symbols, status = Open3.capture2e("objdump", "-T", binary)
abort "objdump -T could not read #{binary}" unless status.success?
versions = symbols.scan(/\bGLIBC_(\d+(?:\.\d+)+)\b/).flatten.uniq
abort "#{binary} references no GLIBC_ symbol versions" if versions.empty?

required = versions.max_by { |version| Gem::Version.new(version) }
if Gem::Version.new(required) > Gem::Version.new(floor)
  abort "#{binary} requires glibc #{required}, above the documented floor #{floor}"
end
puts "#{File.basename(binary)} requires glibc #{required}; the documented floor is #{floor}"
