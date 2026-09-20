#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "json"
require_relative "lib/publication_policy"

abort "usage: prepare-sbom.rb OUTPUT_DIRECTORY SOURCE_ROOT INPUT..." if ARGV.length < 3
output, root, *inputs = ARGV
prefix = "path+file://#{File.expand_path(root).delete_suffix('/')}"

def portable(value, prefix)
  case value
  when Hash then value.transform_values { |item| portable(item, prefix) }
  when Array then value.map { |item| portable(item, prefix) }
  when String
    value.gsub(/#{Regexp.escape(prefix)}(?=\/|#|\z)/, "leani-workspace:")
  else value
  end
end

names = inputs.map { |input| File.basename(input) }
abort "SBOM asset names must be unique" unless names.uniq.length == names.length
FileUtils.mkdir_p(output)
inputs.each do |input|
  name = File.basename(input)
  abort "expected a CycloneDX JSON filename" unless name.end_with?(".cdx.json")
  document = JSON.parse(File.read(input))
  abort "expected a CycloneDX document" unless document["bomFormat"] == "CycloneDX"
  contents = JSON.pretty_generate(portable(document, prefix)) + "\n"
  findings = PublicationPolicy.content_findings(contents)
  abort "#{name}: SBOM still contains private content (redacted)" unless findings.empty?
  File.write(File.join(output, name), contents)
end
puts "prepared #{inputs.length} portable SBOM assets"
