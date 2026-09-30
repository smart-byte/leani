#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "open3"

# Static mode: enforce the protected-publication structure of the workflows.
if ARGV.first == "--workflows"
  abort "usage: check-release-ci.rb --workflows [DIRECTORY]" if ARGV.length > 2
  require_relative "lib/release_workflow_policy"
  directory = ARGV[1] || File.expand_path("../.github/workflows", __dir__)
  errors = ReleaseWorkflowPolicy.violations(directory)
  abort errors.uniq.join("\n") unless errors.empty?
  puts "release workflow policy passed for #{directory}"
  exit
end

repository = ENV.fetch("GITHUB_REPOSITORY")
commit = ENV.fetch("GITHUB_SHA")
abort "invalid repository" unless repository.match?(%r{\A[\w.-]+/[\w.-]+\z})
abort "invalid commit" unless commit.match?(/\A[0-9a-f]{40}\z/)

def api(path)
  output, status = Open3.capture2e("gh", "api", path)
  abort "unable to verify release CI through GitHub" unless status.success?
  JSON.parse(output)
end

abort "usage: check-release-ci.rb [--container | --workflows [DIRECTORY]]" unless ARGV.empty? || ARGV == ["--container"]
workflows = %w[ci.yml security.yml site.yml]
workflows.each do |workflow|
  response = api("repos/#{repository}/actions/workflows/#{workflow}/runs?head_sha=#{commit}&per_page=100")
  runs = response.fetch("workflow_runs").select do |run|
    run["head_sha"] == commit && %w[push workflow_dispatch].include?(run["event"])
  end
  latest = runs.max_by { |run| run.fetch("id") }
  unless latest && latest["status"] == "completed" && latest["conclusion"] == "success"
    abort "#{workflow} must pass on #{commit}; dispatch it on the release ref if no run exists"
  end
  puts "#{workflow}: passed on #{commit}"
end

verify_candidate = lambda do |candidate_id, workflow, artifacts|
  abort "a verified #{workflow} candidate run ID is required" unless candidate_id.match?(/\A[1-9][0-9]*\z/)
  run = api("repos/#{repository}/actions/runs/#{candidate_id}")
  events = workflow == "container.yml" ? %w[push workflow_dispatch] : %w[workflow_dispatch]
  unless run["head_sha"] == commit && events.include?(run["event"]) &&
         run["status"] == "completed" && run["conclusion"] == "success" &&
         run["path"] == ".github/workflows/#{workflow}"
    abort "candidate must be a successful #{workflow} run from the exact release commit"
  end
  inventory = api("repos/#{repository}/actions/runs/#{candidate_id}/artifacts?per_page=100")
  names = inventory.fetch("artifacts").reject { |item| item.fetch("expired") }.map { |item| item.fetch("name") }
  abort "candidate #{candidate_id} lacks its unexpired preparation artifacts" unless
    inventory.fetch("total_count") <= 100 && artifacts.all? { |wanted| names.any? { |name| File.fnmatch(wanted, name) } }
  puts "candidate #{candidate_id}: passed on #{commit}"
end

# A publication run waiting for approval must not invalidate a successful
# container preparation. Its explicit ID binds every publisher to the same
# tested images, and the artifact inventory rejects a publication-only run.
container = ARGV == ["--container"]
container_id = ENV.fetch(container ? "CANDIDATE_RUN_ID" : "CONTAINER_CANDIDATE_RUN_ID", "")
verify_candidate.call(container_id, "container.yml", %w[leani-container-amd64 leani-container-arm64])

candidate_id = ENV.fetch("CANDIDATE_RUN_ID", "")
unless container || candidate_id.empty?
  workflow = ENV.fetch("CANDIDATE_WORKFLOW", "release.yml")
  artifacts = {
    "release.yml" => ["leani-release-candidate-#{ENV.fetch('RELEASE_VERSION', 'v*')}"],
    "sdk-release.yml" => ["leani-sdk-candidate"],
    "crates-release.yml" => ["leani-rust-library-candidate"],
  }
  abort "invalid candidate workflow" unless artifacts.key?(workflow)
  verify_candidate.call(candidate_id, workflow, artifacts.fetch(workflow))
end
