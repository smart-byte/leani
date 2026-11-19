#!/usr/bin/env ruby
# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "open3"
require "rbconfig"
require "tmpdir"
require "yaml"

checker = File.expand_path("check-release-ci.rb", __dir__)
sha = "a" * 40
passed = { "id" => 1, "head_sha" => sha, "event" => "push", "status" => "completed", "conclusion" => "success" }
candidate = passed.merge("event" => "workflow_dispatch", "path" => ".github/workflows/release.yml")
artifacts = { "total_count" => 5, "artifacts" => %w[leani-container-amd64 leani-container-arm64 leani-release-candidate-v0.1.0-rc.2 leani-sdk-candidate leani-rust-library-candidate].map { |name| { "name" => name, "expired" => false } } }
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
  ["container publication waiting for approval does not invalidate its prepared candidate", { "workflow_runs" => [passed] }, candidate, true, [],
   { "container_checks" => { "workflow_runs" => [passed, passed.merge("id" => 2, "status" => "waiting", "conclusion" => nil)] } }],
  ["expired candidates cannot authorize publication", { "workflow_runs" => [passed] }, candidate, false, [],
   { "artifacts" => { "total_count" => 0, "artifacts" => [] }, "expected_error" => "candidate 123 lacks its unexpired preparation artifacts" }],
  ["publication-only container run cannot authorize other publishers", { "workflow_runs" => [passed] }, candidate, false, [],
   { "container_artifacts" => { "total_count" => 0, "artifacts" => [] }, "expected_error" => "candidate 456 lacks its unexpired preparation artifacts" }],
  ["verified SDK candidate is accepted", { "workflow_runs" => [passed] }, candidate.merge("path" => ".github/workflows/sdk-release.yml"), true, [],
   { "candidate_workflow" => "sdk-release.yml" }],
  ["binary candidate cannot authorize SDK publication", { "workflow_runs" => [passed] }, candidate, false, [],
   { "candidate_workflow" => "sdk-release.yml", "expected_error" => "candidate must be a successful sdk-release.yml run" }],
]

Dir.mktmpdir("leani-release-ci-test-") do |directory|
  shim = File.join(directory, "gh")
  File.write(shim, <<~RUBY)
    #!#{RbConfig.ruby}
    require "json"
    data = JSON.parse(File.read(ENV.fetch("RELEASE_TEST_FIXTURE")))
    path = ARGV.last
    key = if path.include?("/actions/runs/456/artifacts?")
            "container_artifacts"
          elsif path.include?("/artifacts?")
            "artifacts"
          elsif path.include?("/actions/runs/456")
            "container"
          elsif path.include?("/actions/runs/")
            "candidate"
          elsif path.include?("/workflows/container.yml/")
            "container_checks"
          else
            "checks"
          end
    puts JSON.generate(data.fetch(key))
  RUBY
  File.chmod(0o755, shim)
  fixture = File.join(directory, "responses.json")
  environment = {
    "PATH" => "#{directory}:#{ENV.fetch('PATH')}",
    "GITHUB_REPOSITORY" => "example/leani",
    "GITHUB_SHA" => sha,
    "CANDIDATE_RUN_ID" => "123",
    "CONTAINER_CANDIDATE_RUN_ID" => "456",
    "RELEASE_TEST_FIXTURE" => fixture,
  }
  cases.each do |name, checks, candidate_run, success, arguments, overrides|
    data = { "checks" => checks, "container_checks" => checks, "candidate" => candidate_run,
             "container" => passed.merge("path" => ".github/workflows/container.yml"), "artifacts" => artifacts,
             "container_artifacts" => artifacts }.merge(overrides || {})
    File.write(fixture, JSON.generate(data))
    configured = environment.merge("CANDIDATE_WORKFLOW" => data["candidate_workflow"])
    output, status = Open3.capture2e(configured, RbConfig.ruby, checker, *Array(arguments))
    raise "#{name}: unexpected result: #{output}" unless status.success? == success
    if data["expected_error"] && !output.include?(data["expected_error"])
      raise "#{name}: rejected for another reason: #{output}"
    end
    puts "PASS #{name}"
  end
end
puts "#{cases.length} release authorization scenarios passed"

# Each scenario changes one property of the repository's own workflows. A
# rejection must name the violated rule, so an unrelated failure cannot pass.
workflows = File.expand_path("../.github/workflows", __dir__)
job = ->(document, name) { document.fetch("jobs").fetch(name) }
step = lambda do |document, name, pattern|
  job.call(document, name).fetch("steps").find do |item|
    item["uses"].to_s.start_with?(pattern) || item["run"].to_s.include?(pattern)
  end
end
# Replaces the SDK publish job's `npm publish ./*.tgz` with another command.
npm_publish = lambda do |command|
  lambda do |document|
    run = step.call(document, "publish", "npm publish").fetch("run")
    run.sub!(%r{npm publish \./\*\.tgz}, command) or raise "npm publish mutation did not apply"
  end
end
npm_rejection = "sdk-release.yml publish: publish jobs only publish a tested artifact; found npm publish without a local archive"
policy_cases = [
  ["repository workflows satisfy the release policy", nil, nil, nil],
  ["publish job without an environment is rejected", "release.yml",
   ->(document) { job.call(document, "publish").delete("environment") },
   "release.yml publish: must run in the release-github environment"],
  ["publish job in another environment is rejected", "sdk-release.yml",
   ->(document) { job.call(document, "publish")["environment"] = "release-crates" },
   "sdk-release.yml publish: must run in the release-npm environment"],
  ["renamed publish job is rejected", "crates-release.yml",
   ->(document) { document["jobs"]["upload"] = document["jobs"].delete("publish") },
   "crates-release.yml: missing protected publish job publish"],
  ["environment on another job is rejected", "ci.yml",
   ->(document) { job.call(document, "sdk")["environment"] = "release-npm" },
   "ci.yml sdk: environment release-npm is reserved for protected publish jobs"],
  ["OIDC in a build job is rejected", "sdk-release.yml",
   ->(document) { job.call(document, "package").fetch("permissions")["id-token"] = "write" },
   "sdk-release.yml package: only protected publish jobs may request id-token: write"],
  ["write access in a build job is rejected", "release.yml",
   ->(document) { job.call(document, "assemble")["permissions"] = { "contents" => "write" } },
   "release.yml assemble: only protected publish jobs may request contents: write"],
  ["write access while building the site is rejected", "site-promote.yml",
   ->(document) { job.call(document, "validate")["permissions"] = { "contents" => "write" } },
   "site-promote.yml validate: only protected publish jobs may request contents: write"],
  ["workflow-wide write access is rejected", "container.yml",
   ->(document) { document.fetch("permissions")["packages"] = "write" },
   "container.yml: workflow permissions must be declared and read-only"],
  ["missing workflow permissions are rejected", "site.yml",
   ->(document) { document.delete("permissions") },
   "site.yml: workflow permissions must be declared and read-only"],
  ["extra publish permission is rejected", "sdk-release.yml",
   ->(document) { job.call(document, "publish").fetch("permissions")["contents"] = "write" },
   "sdk-release.yml publish: permissions must be exactly id-token: write, actions: read"],
  ["persisted checkout credentials are rejected", "ci.yml",
   ->(document) { step.call(document, "rust", "actions/checkout@").fetch("with").delete("persist-credentials") },
   "ci.yml rust: checkout must set persist-credentials: false"],
  ["secrets in a build job are rejected", "crates-release.yml",
   ->(document) { step.call(document, "package", "cargo test")["env"] = { "CARGO_REGISTRY_TOKEN" => "${{ secrets.CARGO_REGISTRY_TOKEN }}" } },
   "crates-release.yml package: only protected publish jobs may read secrets"],
  ["workflow-wide secrets are rejected", "release.yml",
   ->(document) { document.fetch("env")["GH_TOKEN"] = "${{ secrets.RELEASE_TOKEN }}" },
   "release.yml: only protected publish jobs may read secrets"],
  ["publish job that installs dependencies is rejected", "sdk-release.yml",
   ->(document) { job.call(document, "publish").fetch("steps").insert(1, { "run" => "bun install --frozen-lockfile" }) },
   "sdk-release.yml publish: publish jobs only publish a tested artifact; found bun"],
  ["publish job that compiles is rejected", "crates-release.yml",
   ->(document) { job.call(document, "publish").fetch("steps").insert(1, { "run" => "cargo build --locked --release" }) },
   "crates-release.yml publish: publish jobs only publish a tested artifact; found cargo build"],
  ["verifying cargo publish is rejected", "crates-release.yml",
   ->(document) { step.call(document, "publish", "cargo publish").fetch("run").sub!("--no-verify", "--locked") },
   "crates-release.yml publish: publish jobs only publish a tested artifact; found cargo publish without --no-verify"],
  ["publish job that builds an image is rejected", "container.yml",
   ->(document) { job.call(document, "publish").fetch("steps").insert(0, { "uses" => "docker/build-push-action@#{'0' * 40}" }) },
   "container.yml publish: publish jobs only publish a tested artifact; found docker/build-push-action"],
  ["publish job without a downloaded artifact is rejected", "release.yml",
   ->(document) { job.call(document, "publish").fetch("steps").reject! { |item| item["uses"].to_s.start_with?("actions/download-artifact@") } },
   "release.yml publish: publish jobs must download the tested artifact"],
  ["publish job downloading an artifact no build job uploads is rejected", "sdk-release.yml",
   ->(document) { step.call(document, "publish", "actions/download-artifact@").fetch("with")["name"] = "leani-sdk-unverified" },
   "sdk-release.yml publish: publish jobs must download the tested artifact"],
  ["publish job downloading its own upload is rejected", "crates-release.yml",
   lambda do |document|
     upload = step.call(document, "package", "actions/upload-artifact@")
     job.call(document, "package").fetch("steps").delete(upload)
     job.call(document, "publish").fetch("steps").unshift(upload)
   end,
   "crates-release.yml publish: publish jobs must download the tested artifact"],
  ["publish job that does not wait for verification is rejected", "container.yml",
   ->(document) { job.call(document, "publish").delete("needs") },
   "container.yml publish: publish jobs must need the jobs that verify their input"],
  ["checkout in a publish job other than crates is rejected", "release.yml",
   lambda do |document|
     checkout = { "uses" => "actions/checkout@#{'0' * 40}", "with" => { "persist-credentials" => false } }
     job.call(document, "publish").fetch("steps").unshift(checkout)
   end,
   "release.yml publish: publish jobs must not check out the repository"],
  ["npm publish of the working directory is rejected", "sdk-release.yml",
   npm_publish.call("npm publish"), npm_rejection],
  ["npm publish with an option value named like an archive is rejected", "sdk-release.yml",
   npm_publish.call("npm publish --tag x.tgz"), npm_rejection],
  ["npm publish after an option is rejected", "sdk-release.yml",
   npm_publish.call("npm --provenance publish ."), npm_rejection],
  ["npm publish of a remote archive is rejected", "sdk-release.yml",
   npm_publish.call("npm publish https://registry.example/leani-sdk.tgz"), npm_rejection],
  ["npm run by absolute path is caught", "sdk-release.yml",
   npm_publish.call("/usr/bin/npm publish ."), npm_rejection],
  ["npm run by relative path is caught", "sdk-release.yml",
   npm_publish.call("./node_modules/.bin/npm publish ."), npm_rejection],
  ["npm publish of a hosted repository archive is rejected", "sdk-release.yml",
   npm_publish.call("npm publish user/repo#x.tgz"), npm_rejection],
  ["npm publish of a scoped package name is rejected", "sdk-release.yml",
   npm_publish.call("npm publish @scope/x.tgz"), npm_rejection],
  ["npm publish of a bare slash path is rejected", "sdk-release.yml",
   npm_publish.call("npm publish foo/x.tgz"), npm_rejection],
  ["pull_request_target trigger is rejected", "container.yml",
   ->(document) { (document["on"] || document[true])["pull_request_target"] = nil },
   "container.yml: workflows must not trigger on pull_request_target"],
  ["a later push to main cannot cancel the run a release needs", "ci.yml",
   ->(document) { document.fetch("concurrency")["group"] = "ci-${{ github.ref }}" },
   "ci.yml: concurrency must give each main commit its own group"],
  ["workflow_run trigger is rejected", "site.yml",
   ->(document) { (document["on"] || document[true])["workflow_run"] = { "workflows" => ["CI"], "types" => ["completed"] } },
   "site.yml: workflows must not trigger on workflow_run"],
  ["release controller cannot run from unreviewed branches", "release-coordinate.yml",
   ->(document) { job.call(document, "coordinate")["if"] = "inputs.action != 'prepare'" },
   "release-coordinate.yml coordinate: release control must dispatch from main"],
  ["release controller cannot grant registry OIDC", "release-coordinate.yml",
   ->(document) { job.call(document, "coordinate").fetch("permissions")["id-token"] = "write" },
   "release-coordinate.yml coordinate: permissions must be exactly contents: read, actions: read"],
  ["release controller main restriction cannot be bypassed by another condition", "release-coordinate.yml",
   ->(document) { job.call(document, "coordinate")["if"] += " || true" },
   "release-coordinate.yml coordinate: release control must dispatch from main"],
]

Dir.mktmpdir("leani-release-workflows-test-") do |directory|
  policy_cases.each_with_index do |(name, file, mutation, message), index|
    copy = File.join(directory, index.to_s)
    FileUtils.mkdir_p(copy)
    Dir.glob(File.join(workflows, "*.{yml,yaml}")).each { |path| FileUtils.cp(path, copy) }
    if file
      path = File.join(copy, file)
      document = YAML.safe_load(File.read(path))
      mutation.call(document)
      File.write(path, YAML.dump(document))
    end
    output, status = Open3.capture2e(RbConfig.ruby, checker, "--workflows", copy)
    if message
      raise "#{name}: accepted: #{output}" if status.success?
      raise "#{name}: rejected for another reason: #{output}" unless output.include?(message)
    else
      raise "#{name}: rejected: #{output}" unless status.success?
    end
    puts "PASS #{name}"
  end
end
puts "#{policy_cases.length} release workflow policy scenarios passed"

# The container publisher's push step runs against a fake registry. A release
# tag that holds another image stops it before any push, and a retry keeps the
# tags that already hold the verified images.
push_step = YAML.safe_load(File.read(File.join(workflows, "container.yml")))
                .dig("jobs", "publish", "steps").find { |item| item["id"] == "push" }.fetch("run")
image = "ghcr.io/example/leani"
tag = "v0.1.0-rc.2"
identities = { "amd64" => "sha256:#{'1' * 64}", "arm64" => "sha256:#{'2' * 64}" }
held = ->(arch, identity = identities[arch]) { { "config" => { "digest" => identity }, "digest" => "sha256:held-#{arch}" } }
held_index = { "manifests" => %w[amd64 arm64].map { |arch| { "digest" => "sha256:held-#{arch}" } }, "digest" => "sha256:held-index" }
container_cases = [
  ["a new release pushes both images and assembles their tag", {},
   ["push #{image}:#{tag}-amd64", "push #{image}:#{tag}-arm64", "create #{image}:#{tag}"], nil],
  ["a retry keeps an image tag that holds its verified image", { "#{image}:#{tag}-amd64" => held.call("amd64") },
   ["push #{image}:#{tag}-arm64", "create #{image}:#{tag}"], nil],
  ["a complete publication is left as it is",
   { "#{image}:#{tag}-amd64" => held.call("amd64"), "#{image}:#{tag}-arm64" => held.call("arm64"), "#{image}:#{tag}" => held_index },
   [], nil],
  ["an image tag that holds another image is never moved",
   { "#{image}:#{tag}-arm64" => held.call("arm64", "sha256:#{'3' * 64}") }, [], "#{image}:#{tag}-arm64 already holds another image"],
  ["a multiarchitecture tag that holds other images is never moved", { "#{image}:#{tag}" => held_index },
   [], "#{image}:#{tag} already holds other images"],
]

Dir.mktmpdir("leani-container-publish-test-") do |directory|
  shim = File.join(directory, "docker")
  File.write(shim, "#!#{RbConfig.ruby}\n" + <<~'RUBY')
    require "json"
    path = ENV.fetch("DOCKER_TEST_STATE")
    state = JSON.parse(File.read(path))
    registry = state["registry"]
    case ARGV
    in ["load", "--input", archive] then state["loaded"] = archive[/leani-container-(\w+)\.tar\.gz\z/, 1]
    in ["image", "inspect", "leani:scan", "--format", "{{.Id}}"] then puts state["images"].fetch(state["loaded"])
    in ["image", "inspect", "leani:scan", "--format", "{{.Architecture}}"] then puts state["loaded"]
    in ["tag", "leani:scan", _] then nil
    in ["push", ref]
      registry[ref] = { "config" => { "digest" => state["images"].fetch(state["loaded"]) }, "digest" => "sha256:pushed-#{state['loaded']}" }
      state["writes"] << "push #{ref}"
    in ["image", "inspect", ref, "--format", "{{index .RepoDigests 0}}"] then puts "#{ref.rpartition(':').first}@#{registry.fetch(ref)['digest']}"
    in ["buildx", "imagetools", "create", "--tag", ref, *sources]
      registry[ref] = { "manifests" => sources.map { |source| { "digest" => source.split("@").last } }, "digest" => "sha256:index" }
      state["writes"] << "create #{ref}"
    in ["buildx", "imagetools", "inspect", ref, *options]
      entry = registry[ref] or abort("ERROR: #{ref}: not found")
      puts(case options
           in ["--raw"] then JSON.generate(entry.except("digest"))
           in ["--format", _] then JSON.generate("digest" => entry["digest"])
           in [] then ref
           end)
    end
    File.write(path, JSON.generate(state))
  RUBY
  File.chmod(0o755, shim)
  container_cases.each_with_index do |(name, registry, writes, error), index|
    work = File.join(directory, index.to_s)
    identities.each do |arch, identity|
      candidate = File.join(work, "images", "leani-container-#{arch}")
      FileUtils.mkdir_p(candidate)
      files = { "leani-container-#{arch}.tar.gz" => arch, "leani-container-#{arch}.cdx.json" => "{}", "image.id" => "#{identity}\n" }
      files.each { |file, contents| File.write(File.join(candidate, file), contents) }
      File.write(File.join(candidate, "SHA256SUMS"), files.map { |file, contents| "#{Digest::SHA256.hexdigest(contents)}  #{file}\n" }.join)
    end
    File.write(File.join(work, "step.sh"), push_step)
    state = File.join(work, "state.json")
    File.write(state, JSON.generate("registry" => registry, "images" => identities, "writes" => []))
    environment = { "PATH" => "#{directory}:#{ENV.fetch('PATH')}", "GITHUB_REPOSITORY" => "Example/leani", "IMAGE_TAG" => tag,
                    "GITHUB_OUTPUT" => File.join(work, "output"), "DOCKER_TEST_STATE" => state }
    output, status = Open3.capture2e(environment, "bash", "--noprofile", "--norc", "-eo", "pipefail", "step.sh", chdir: work)
    raise "#{name}: unexpected result: #{output}" unless status.success? == error.nil?
    raise "#{name}: rejected for another reason: #{output}" if error && !output.include?(error)
    actual = JSON.parse(File.read(state)).fetch("writes")
    raise "#{name}: wrote #{actual.inspect}, expected #{writes.inspect}" unless actual == writes
    puts "PASS #{name}"
  end
end
puts "#{container_cases.length} container publication scenarios passed"
