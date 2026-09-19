#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "open3"
require "tmpdir"

scanner = File.expand_path("check-secrets.sh", __dir__)
public_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
fixture_path = "site/src/data/processors/uniswap-latest.json"
sample = ("0123456789abcdef" * 3).chars.shuffle(random: Random.new(7)).join
cases = [
  ["public address in its reviewed fixture", fixture_path, public_address, true, false],
  ["another key in the same fixture", fixture_path, sample, false, false],
  ["same address in an unrelated credential file", "settings.json", public_address, false, false],
  ["deleted credential remains detectable", "settings.json", sample, false, true],
]

def git(*arguments)
  output, status = Open3.capture2e("git", *arguments)
  raise "git #{arguments.first} failed" unless status.success?
  output
end

cases.each do |name, path, value, success, delete|
  Dir.mktmpdir("leani-secret-scan-test-") do |directory|
    Dir.chdir(directory) do
      git("init", "-q", "-b", "main")
      git("config", "user.name", "Publication Test")
      git("config", "user.email", "test@example.invalid")
      git("config", "commit.gpgsign", "false")
      git("config", "core.hooksPath", "/dev/null")
      FileUtils.mkdir_p(File.dirname(path))
      File.write(path, "{\"key\": \"#{value}\"}\n")
      git("add", ".")
      git("commit", "-qm", "Test fixture")
      if delete
        git("rm", path)
        git("commit", "-qm", "Remove fixture")
      end
      output, status = Open3.capture2e(scanner, "HEAD")
      raise "#{name}: unexpected scanner result: #{output}" unless status.success? == success
      raise "scanner disclosed a matched value" if !success && output.include?(value)
    end
  end
  puts "PASS #{name}"
end
puts "#{cases.length} credential scanner scenarios passed"
