#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "tmpdir"

checker = File.expand_path("check-artifact-privacy.rb", __dir__)
preparer = File.expand_path("prepare-sbom.rb", __dir__)
count = 0
check = lambda do |name, expected, script, *arguments|
  output, status = Open3.capture2e(RbConfig.ruby, script, *arguments)
  raise "#{name}: unexpected result: #{output}" unless status.success? == expected
  puts "PASS #{name}"
  count += 1
  output
end

Dir.mktmpdir("leani-artifact-test-") do |directory|
  public_file = File.join(directory, "README.md")
  File.write(public_file, "Public distribution\n")
  check.call("public contents pass", true, checker, public_file)
  check.call("missing input fails closed", false, checker, File.join(directory, "missing"))
  empty = File.join(directory, "empty")
  FileUtils.mkdir_p(empty)
  check.call("empty directory fails closed", false, checker, empty)

  private_root = "/" + %w[home release-runner checkout].join("/")
  binary = File.join(directory, "leani")
  File.binwrite(binary, "\x00\xff#{private_root}/src/lib.rs\x00")
  output = check.call("binary build paths are rejected", false, checker, binary)
  raise "diagnostics exposed the matched content" if output.include?(private_root)
  File.delete(binary)

  plan = File.join(directory, "plans", "launch.md")
  FileUtils.mkdir_p(File.dirname(plan))
  File.write(plan, "Internal notes\n")
  check.call("private artifact paths are rejected", false, checker, directory)
  FileUtils.rm_rf(File.dirname(plan))
  link = File.join(directory, "linked.md")
  File.symlink(public_file, link)
  check.call("symlinks are rejected", false, checker, link)
  File.delete(link)

  reference = "path+file://#{private_root}/crates/node#leani@0.1.0-rc.1"
  document = {
    "bomFormat" => "CycloneDX", "specVersion" => "1.5",
    "metadata" => { "component" => { "bom-ref" => reference } },
    "components" => [{ "bom-ref" => reference }],
    "dependencies" => [{ "ref" => reference, "dependsOn" => [reference] }],
  }
  source = File.join(directory, "node.cdx.json")
  File.write(source, JSON.generate(document))
  destination = File.join(directory, "assets")
  check.call("SBOM source references become portable", true, preparer, destination, private_root, source)
  normalized = JSON.parse(File.read(File.join(destination, "node.cdx.json")))
  refs = [normalized.dig("metadata", "component", "bom-ref"), normalized["components"][0]["bom-ref"],
          normalized["dependencies"][0]["ref"], normalized["dependencies"][0]["dependsOn"][0]]
  raise "SBOM dependency references diverged" unless refs.uniq == ["leani-workspace:/crates/node#leani@0.1.0-rc.1"]
  check.call("portable SBOM passes privacy check", true, checker, destination)
  check.call("duplicate SBOM asset names are rejected", false, preparer, destination, private_root, source, source)
  document["unrelated"] = private_root + "-other/src/lib.rs"
  File.write(source, JSON.generate(document))
  check.call("unrecognized absolute paths fail closed", false, preparer, destination, private_root, source)
end
puts "#{count} artifact privacy scenarios passed"
