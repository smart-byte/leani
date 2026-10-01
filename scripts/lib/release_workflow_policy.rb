# frozen_string_literal: true

require "json"
require "yaml"

# Static rules for every GitHub Actions workflow. Only the publish jobs listed
# here may hold write permissions, OIDC tokens, or secrets. Each runs in its
# protected GitHub environment and only publishes what earlier, unprivileged
# jobs built and tested. Changing a permission set here is a release-policy
# change: document it in docs/contributing/releasing.md.
module ReleaseWorkflowPolicy
  # Crates.io repackages the tested commit. The coordinator checks out reviewed
  # release control scripts and writes only with its release-control credential;
  # its workflow token stays read-only.
  PUBLISH_JOBS = {
    "release.yml" => {
      "publish" => {
        environment: "release-github",
        artifact: true,
        permissions: { "contents" => "write", "actions" => "read", "id-token" => "write", "attestations" => "write" },
      },
    },
    "container.yml" => {
      "publish" => {
        environment: "release-ghcr",
        artifact: true,
        permissions: { "packages" => "write", "actions" => "read", "id-token" => "write", "attestations" => "write" },
      },
    },
    "sdk-release.yml" => {
      "publish" => { environment: "release-npm", artifact: true, permissions: { "id-token" => "write", "actions" => "read" } },
    },
    "crates-release.yml" => {
      "publish" => {
        environment: "release-crates",
        artifact: true,
        checkout: true,
        permissions: { "contents" => "read", "id-token" => "write", "actions" => "read" },
      },
    },
    "site-promote.yml" => {
      "promote" => { environment: "site-production", artifact: false, permissions: { "contents" => "write" } },
    },
    "release-coordinate.yml" => {
      "prepare" => {
        environment: "release-control", artifact: true, checkout: true, controller: true,
        condition: "github.ref == 'refs/heads/main' && inputs.action == 'prepare'",
        permissions: { "contents" => "read" },
      },
      "coordinate" => {
        environment: "release-control", artifact: false, checkout: true, controller: true,
        condition: "github.ref == 'refs/heads/main' && inputs.action != 'prepare'",
        permissions: { "contents" => "read", "actions" => "read" },
      },
    },
  }.freeze

  READ_ONLY_ACCESS = %w[read none].freeze
  # Actions that build, restore caches, or install toolchains for builds.
  BUILD_ACTIONS = %w[actions/cache docker/build-push-action oven-sh/setup-bun].freeze
  ALLOWED_INSTALLS = /\bnpm install --global npm@\d+(?:\.\d+)*(?=\s|\z)/
  BUILD_COMMANDS = {
    "bun" => /(?<![\w.\/-])bun(?![\w.-])/,
    "npx" => /(?<![\w.\/-])npx(?![\w.-])/,
    "a package install or script" => /\b(?:npm|pnpm|yarn)\s+(?:ci|i|install|add|run|run-script|test|exec|pack|rebuild)\b/,
    "docker build" => /\bdocker\s+(?:buildx\s+)?build\b/,
  }.freeze
  # One npm command: the words after `npm` up to a shell separator. npm reads
  # options before its subcommand, so `publish` may follow any of them. npm
  # run by path, such as `/usr/bin/npm`, counts too.
  NPM_COMMAND = /(?<![\w.-])npm\s+([^;&|]*)/
  SECRET_EXPRESSION = /\$\{\{[^}]*\bsecrets\b/
  # Both run with the base repository's token, secrets, and cache scope on
  # behalf of code or events that a pull request controls.
  FORBIDDEN_TRIGGERS = %w[pull_request_target workflow_run].freeze

  def self.violations(directory)
    paths = Dir.glob(File.join(directory, "*.{yml,yaml}")).sort
    return ["no workflows found in #{directory}"] if paths.empty?

    documents = paths.to_h { |path| [File.basename(path), YAML.safe_load(File.read(path))] }
    errors = []
    PUBLISH_JOBS.each do |file, jobs|
      jobs.each_key do |name|
        errors << "#{file}: missing protected publish job #{name}" unless documents.dig(file, "jobs", name)
      end
    end
    documents.each { |file, document| check_workflow(file, document, errors) }
    errors
  end

  def self.check_workflow(file, document, errors)
    (triggers(document) & FORBIDDEN_TRIGGERS).each do |trigger|
      errors << "#{file}: workflows must not trigger on #{trigger}"
    end
    errors << "#{file}: workflow permissions must be declared and read-only" unless read_only?(document["permissions"])
    errors << "#{file}: only protected publish jobs may read secrets" if secrets?(document.except("jobs"))
    jobs = document.fetch("jobs", {})
    jobs.each do |name, job|
      check_checkouts(file, name, job, errors)
      publisher = PUBLISH_JOBS.dig(file, name)
      if publisher
        uploads = jobs.except(name).values.flat_map { |other| artifact_names(other, "upload") }
        check_publisher(file, name, job, publisher, uploads, errors)
      else
        check_unprivileged(file, name, job, errors)
      end
    end
  end

  # YAML 1.1 reads the unquoted key `on` as true.
  def self.triggers(document)
    case (value = document.key?("on") ? document["on"] : document[true])
    when Hash then value.keys.map(&:to_s)
    when Array then value.map(&:to_s)
    when nil then []
    else [value.to_s]
    end
  end

  def self.artifact_names(job, direction)
    job.fetch("steps", []).filter_map do |step|
      next unless step["uses"].to_s.start_with?("actions/#{direction}-artifact@")

      step.dig("with", "name") || step.dig("with", "pattern")
    end
  end

  def self.read_only?(permissions)
    case permissions
    when "read-all" then true
    when Hash then permissions.values.all? { |access| READ_ONLY_ACCESS.include?(access) }
    else false
    end
  end

  def self.secrets?(value)
    JSON.generate(value).match?(SECRET_EXPRESSION)
  end

  def self.environment_name(job)
    environment = job["environment"]
    environment.is_a?(Hash) ? environment["name"] : environment
  end

  def self.check_checkouts(file, name, job, errors)
    job.fetch("steps", []).each do |step|
      next unless step["uses"].to_s.start_with?("actions/checkout@")
      next if [false, "false"].include?(step.dig("with", "persist-credentials"))

      errors << "#{file} #{name}: checkout must set persist-credentials: false"
    end
  end

  def self.check_unprivileged(file, name, job, errors)
    if (environment = environment_name(job))
      errors << "#{file} #{name}: environment #{environment} is reserved for protected publish jobs"
    end
    case (permissions = job["permissions"])
    when Hash
      permissions.each do |scope, access|
        errors << "#{file} #{name}: only protected publish jobs may request #{scope}: #{access}" unless READ_ONLY_ACCESS.include?(access)
      end
    when nil, "read-all" then nil
    else errors << "#{file} #{name}: only protected publish jobs may request #{permissions}"
    end
    errors << "#{file} #{name}: only protected publish jobs may read secrets" if job.key?("secrets") || secrets?(job)
  end

  # A publish job must download what another job of the same workflow
  # uploaded: in this run, or in the preparation run it names by run-id.
  def self.check_publisher(file, name, job, publisher, uploads, errors)
    if publisher[:controller] && job["if"] != publisher[:condition]
      errors << "#{file} #{name}: release control must dispatch from main"
    end
    unless environment_name(job) == publisher[:environment]
      errors << "#{file} #{name}: must run in the #{publisher[:environment]} environment"
    end
    unless job["permissions"] == publisher[:permissions]
      expected = publisher[:permissions].map { |scope, access| "#{scope}: #{access}" }.join(", ")
      errors << "#{file} #{name}: permissions must be exactly #{expected}"
    end
    if Array(job["needs"]).empty?
      errors << "#{file} #{name}: publish jobs must need the jobs that verify their input"
    end
    handed_over = artifact_names(job, "download").any? do |wanted|
      uploads.any? { |uploaded| uploaded == wanted || File.fnmatch(wanted, uploaded) }
    end
    if publisher[:artifact] && !handed_over
      errors << "#{file} #{name}: publish jobs must download the tested artifact"
    end
    job.fetch("steps", []).each do |step|
      if step["uses"].to_s.start_with?("actions/checkout@") && !publisher[:checkout]
        errors << "#{file} #{name}: publish jobs must not check out the repository"
      end
      build_steps(step).each do |found|
        errors << "#{file} #{name}: publish jobs only publish a tested artifact; found #{found}"
      end
    end
  end

  def self.build_steps(step)
    action = step["uses"].to_s.split("@").first
    return [action] if BUILD_ACTIONS.include?(action)

    script = step["run"].to_s.gsub(/\\\n[ \t]*/, " ").gsub(ALLOWED_INSTALLS, "")
    script.each_line.reject { |line| line.lstrip.start_with?("#") }.flat_map do |line|
      found = BUILD_COMMANDS.filter_map { |label, pattern| label if line.match?(pattern) }
      line.scan(/\bcargo\s+([a-z][\w-]*)/).flatten.each do |command|
        if %w[package publish].include?(command)
          found << "cargo #{command} without --no-verify" unless line.include?("--no-verify")
        else
          found << "cargo #{command}"
        end
      end
      line.scan(NPM_COMMAND).flatten.each do |arguments|
        words = arguments.split.map { |word| word.delete(%("')) }
        next unless (index = words.index("publish"))

        found << "npm publish without a local archive" unless local_archive?(words[index + 1])
      end
      found
    end
  end

  # npm publish of a directory packs it again and runs its lifecycle scripts,
  # and a URL fetches something other than the tested archive. The archive
  # must be the word right after `publish`, so no option value can pose as it.
  # npm reads `user/repo#ref` as a hosted repository and `@scope/name` as a
  # package, so a word with `/`, `@`, or `#` must be an explicit path.
  def self.local_archive?(word)
    word = word.to_s
    return false unless word.end_with?(".tgz") && !word.start_with?("-") && !word.include?(":")

    word.start_with?("./", "../", "/", "~/", "$") || !word.match?(%r{[/@#]})
  end
end
