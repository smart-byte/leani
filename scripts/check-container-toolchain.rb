#!/usr/bin/env ruby
# frozen_string_literal: true

# Fails unless every Rust image stage in the Dockerfile uses the exact Rust
# version that rust-toolchain.toml pins.
abort "usage: check-container-toolchain.rb [DOCKERFILE TOOLCHAIN]" unless [0, 2].include?(ARGV.length)
root = File.expand_path("..", __dir__)
dockerfile, toolchain = ARGV.empty? ? [File.join(root, "Dockerfile"), File.join(root, "rust-toolchain.toml")] : ARGV

channel = File.read(toolchain)[/^channel\s*=\s*"(\d+\.\d+\.\d+)"\s*$/, 1]
abort "#{toolchain} must pin an exact Rust version" unless channel
tags = File.read(dockerfile).scan(/^\s*FROM\s+(?:--platform=\S+\s+)?rust:([^\s@]+)/i).flatten
abort "#{dockerfile} has no Rust image stage" if tags.empty?
tags.each do |tag|
  next if tag.match?(/\A#{Regexp.escape(channel)}(?:-|\z)/)

  abort "#{dockerfile} uses rust:#{tag}, but rust-toolchain.toml pins #{channel}"
end
puts "Dockerfile Rust image matches rust-toolchain.toml #{channel}"
