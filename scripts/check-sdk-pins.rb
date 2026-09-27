#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"

# Fails unless every documented install of the TypeScript SDK pins the exact
# version packages/sdk/package.json declares. The SDK publishes prereleases
# under npm's `next` tag, so an install without a version resolves `latest`,
# which can be an SDK the node no longer supports.
root = File.expand_path("..", __dir__)
abort "usage: check-sdk-pins.rb [PACKAGE_JSON DOCUMENT...]" if ARGV.length == 1
package, *documents =
  if ARGV.empty?
    [
      File.join(root, "packages/sdk/package.json"),
      File.join(root, "README.md"),
      File.join(root, "packages/sdk/README.md"),
      *Dir.glob(File.join(root, "docs/**/*.{md,mdx}")).sort,
    ]
  else
    ARGV
  end

manifest = JSON.parse(File.read(package))
name = manifest.fetch("name")
version = manifest.fetch("version")
install = /\b(?:bun\s+add|npm\s+(?:install|i)|pnpm\s+add|yarn\s+add)\b[^\n`]*?(?<![\w\/.-])#{Regexp.escape(name)}(?:@([^\s`'"]*))?/
errors = []
installs = 0
documents.each do |document|
  File.foreach(document).with_index(1) do |line, number|
    line.scan(install).flatten.each do |pinned|
      installs += 1
      next if pinned == version

      found = pinned ? "@#{pinned}" : "no version"
      errors << "#{document}:#{number}: installs #{name} with #{found}; pin @#{version}"
    end
  end
end
errors << "no documented #{name} install found" if installs.zero?
abort errors.join("\n") unless errors.empty?
puts "#{installs} documented #{name} installs pin #{version}"
