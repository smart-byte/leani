#!/usr/bin/env ruby
# frozen_string_literal: true

require "json"
require "open3"

repository = ENV.fetch("GITHUB_REPOSITORY")
commit = ENV.fetch("GITHUB_SHA")
abort "invalid repository" unless repository.match?(%r{\A[\w.-]+/[\w.-]+\z})
abort "invalid commit" unless commit.match?(/\A[0-9a-f]{40}\z/)

def api(path)
  output, status = Open3.capture2e("gh", "api", path)
  abort "unable to verify release CI through GitHub" unless status.success?
  JSON.parse(output)
end

abort "usage: check-release-ci.rb [--container]" unless ARGV.empty? || ARGV == ["--container"]
workflows = %w[ci.yml security.yml site.yml]
# The container publisher already depends on its current build-and-scan job.
# Requiring its own workflow to have completed here would deadlock publication.
workflows << "container.yml" unless ARGV == ["--container"]
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

candidate_id = ENV.fetch("CANDIDATE_RUN_ID", "")
unless candidate_id.empty?
  abort "invalid candidate run ID" unless candidate_id.match?(/\A[1-9][0-9]*\z/)
  run = api("repos/#{repository}/actions/runs/#{candidate_id}")
  unless run["head_sha"] == commit && run["event"] == "workflow_dispatch" &&
         run["status"] == "completed" && run["conclusion"] == "success" &&
         run["path"] == ".github/workflows/release.yml"
    abort "candidate must be a successful Prepare release run from the exact release commit"
  end
  puts "candidate #{candidate_id}: passed on #{commit}"
end
