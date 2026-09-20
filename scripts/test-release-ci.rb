#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "open3"
require "rbconfig"
require "tmpdir"

checker = File.expand_path("check-release-ci.rb", __dir__)
sha = "a" * 40
passed = { "id" => 1, "head_sha" => sha, "event" => "push", "status" => "completed", "conclusion" => "success" }
candidate = passed.merge("event" => "workflow_dispatch", "path" => ".github/workflows/release.yml")
cases = [
  ["all exact-commit checks pass", { "workflow_runs" => [passed] }, candidate, true],
  ["older commit cannot satisfy checks", { "workflow_runs" => [passed.merge("head_sha" => "b" * 40)] }, candidate, false],
  ["missing checks fail closed", { "workflow_runs" => [] }, candidate, false],
  ["newer failed run overrides older success", { "workflow_runs" => [passed, passed.merge("id" => 2, "conclusion" => "failure")] }, candidate, false],
  ["in-progress rerun blocks publication", { "workflow_runs" => [passed.merge("status" => "in_progress", "conclusion" => nil)] }, candidate, false],
  ["pull-request results cannot authorize publication", { "workflow_runs" => [passed.merge("event" => "pull_request")] }, candidate, false],
  ["candidate from another commit is rejected", { "workflow_runs" => [passed] }, candidate.merge("head_sha" => "b" * 40), false],
  ["candidate from another workflow is rejected", { "workflow_runs" => [passed] }, candidate.merge("path" => ".github/workflows/ci.yml"), false],
  ["container preparation from push is accepted", { "workflow_runs" => [passed] }, passed.merge("path" => ".github/workflows/container.yml"), true, ["--container"]],
  ["container preparation from dispatch is accepted", { "workflow_runs" => [passed] }, candidate.merge("path" => ".github/workflows/container.yml"), true, ["--container"]],
  ["container preparation from a pull request is rejected", { "workflow_runs" => [passed] }, passed.merge("path" => ".github/workflows/container.yml", "event" => "pull_request"), false, ["--container"]],
  ["binary candidate cannot authorize a container", { "workflow_runs" => [passed] }, candidate, false, ["--container"]],
  ["older container candidate is rejected", { "workflow_runs" => [passed] }, passed.merge("path" => ".github/workflows/container.yml", "head_sha" => "b" * 40), false, ["--container"]],
]

Dir.mktmpdir("leani-release-ci-test-") do |directory|
  shim = File.join(directory, "gh")
  File.write(shim, <<~RUBY)
    #!#{RbConfig.ruby}
    require "json"
    data = JSON.parse(File.read(ENV.fetch("RELEASE_TEST_FIXTURE")))
    key = ARGV.last.include?("/actions/runs/") ? "candidate" : "checks"
    puts JSON.generate(data.fetch(key))
  RUBY
  File.chmod(0o755, shim)
  fixture = File.join(directory, "responses.json")
  environment = {
    "PATH" => "#{directory}:#{ENV.fetch('PATH')}",
    "GITHUB_REPOSITORY" => "example/leani",
    "GITHUB_SHA" => sha,
    "CANDIDATE_RUN_ID" => "123",
    "RELEASE_TEST_FIXTURE" => fixture,
  }
  cases.each do |name, checks, candidate_run, success, arguments|
    File.write(fixture, JSON.generate("checks" => checks, "candidate" => candidate_run))
    output, status = Open3.capture2e(environment, RbConfig.ruby, checker, *Array(arguments))
    raise "#{name}: unexpected result: #{output}" unless status.success? == success
    puts "PASS #{name}"
  end
end
puts "#{cases.length} release authorization scenarios passed"
