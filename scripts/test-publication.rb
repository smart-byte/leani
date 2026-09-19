#!/usr/bin/env ruby
# frozen_string_literal: true

require "fileutils"
require "open3"
require "rbconfig"
require "tmpdir"

CHECKER = File.expand_path("check-publication.rb", __dir__)
HOOKS = File.expand_path("git-hooks", __dir__)
ZERO = "0" * 40

def command(*args, input: "")
  Open3.capture2e(*args, stdin_data: input)
end

def git(*args)
  output, status = command("git", *args)
  raise "git #{args.first} failed: #{output}" unless status.success?
  output.strip
end

def write(path, content)
  FileUtils.mkdir_p(File.dirname(path))
  File.write(path, content)
end

def check(*args, success:, input: "", includes: nil, excludes: nil)
  output, status = command(RbConfig.ruby, CHECKER, *args, input: input)
  raise "unexpected checker result: #{output}" unless status.success? == success
  raise "missing diagnostic #{includes}" if includes && !output.include?(includes)
  raise "checker disclosed private content" if excludes && output.include?(excludes)
end

cases = {
  "public code and documentation remain allowed" => lambda {
    write("docs/adr/0001-contract.md", "Public contract and portable /tmp/leani-fixture instructions.\n")
    write("crates/source-api/src/planner.rs", "// Query planner\n")
    git("add", ".")
    check("--staged", success: true)
    git("commit", "-qm", "feat: public example")
    check("HEAD", success: true)
  },
  "staged content is checked even when the working file was cleaned" => lambda {
    private_path = ["", "Users", "fixture-person", "workspace", ""].join("/")
    write("result.txt", private_path)
    git("add", "result.txt")
    write("result.txt", "clean working copy")
    check("--staged", success: false, includes: "local user directory", excludes: private_path)
  },
  "deleted plans still fail historical checks" => lambda {
    write("docs/superpowers/plans/work.md", "Internal work\n")
    git("add", ".")
    git("commit", "-qm", "docs: work")
    git("rm", "docs/superpowers/plans/work.md")
    git("commit", "-qm", "docs: remove work")
    check("--staged", success: true)
    check("HEAD", success: false, includes: "private working file")
    FileUtils.mkdir_p("site")
    Dir.chdir("site") { check("HEAD", success: false, includes: "private working file") }
  },
  "credentials and Windows paths are rejected without echoing them" => lambda {
    token = "gh" + "p_" + "a" * 36
    windows_path = ["C:", "Users", "fixture-person", "project"].join("\\")
    write("output.txt", token + "\n" + windows_path)
    git("add", ".")
    check("--staged", success: false, includes: "provider credential", excludes: token)
    check("--staged", success: false, includes: "local user directory", excludes: windows_path)
  },
  "all pushed refs are inspected, including historical leaks" => lambda {
    clean = git("rev-parse", "HEAD")
    write("reports/internal.md", "internal")
    git("add", ".")
    git("commit", "-qm", "docs: internal")
    dirty = git("rev-parse", "HEAD")
    git("reset", "--hard", clean)
    input = "refs/heads/main #{clean} refs/heads/main #{ZERO}\nrefs/heads/topic #{dirty} refs/heads/topic #{ZERO}\n"
    check("--pre-push", success: false, input: input, includes: "private working file")
    check("--pre-push", success: false, input: "refs/heads/archive/old #{clean} refs/heads/archive/old #{ZERO}\n", includes: "private or recovery ref")
    check("--pre-push", success: true, input: "(delete) #{ZERO} refs/heads/topic #{dirty}\n")
    check("--pre-push", success: false, input: "invalid\n")
  },
  "commit and tag messages are checked" => lambda {
    private_path = ["", "home", "fixture-person", "project", ""].join("/")
    write("message", private_path)
    check("--message", "message", success: false, includes: "commit message", excludes: private_path)
    git("tag", "-a", "v1.0.0", "-m", private_path)
    check("v1.0.0", success: false, includes: "in tag", excludes: private_path)
    git("tag", "-a", "outer", "v1.0.0", "-m", "Release wrapper")
    check("outer", success: false, includes: "in tag", excludes: private_path)
    git("commit", "--allow-empty", "-qm", private_path)
    check("HEAD", success: false, includes: "in commit metadata/message", excludes: private_path)
  },
  "installed hooks stop commits before private files enter history" => lambda {
    git("config", "core.hooksPath", HOOKS)
    write("notes/plan.md", "private")
    git("add", ".")
    output, status = command("git", "commit", "-qm", "docs: unsafe")
    raise "pre-commit hook did not block" if status.success? || !output.include?("private working file")
    git("reset", "--hard", "HEAD")
    secret = "npm" + "_" + "a" * 36
    output, status = command("git", "commit", "--allow-empty", "-qm", secret)
    raise "commit-msg hook did not block" if status.success? || !output.include?("commit message")
    raise "hook disclosed credential" if output.include?(secret)
  },
  "missing refs fail closed" => lambda {
    check("does-not-exist", success: false)
  },
  "pre-push hook blocks an unsafe branch before the remote receives it" => lambda {
    write("reports/private.md", "private work")
    git("add", ".")
    git("commit", "-qm", "docs: unpublished work")
    git("config", "core.hooksPath", HOOKS)
    Dir.mktmpdir("leani-publication-remote-") do |remote|
      git("init", "--bare", "-q", remote)
      output, status = command("git", "push", remote, "main")
      raise "unsafe push was not blocked" if status.success? || !output.include?("private working file")
      raise "unsafe commit reached remote" unless git("ls-remote", remote).empty?
    end
  },
}

cases.each do |name, test|
  Dir.mktmpdir("leani-publication-test-") do |directory|
    Dir.chdir(directory) do
      git("init", "-q", "-b", "main")
      git("config", "user.name", "Publication Test")
      git("config", "user.email", "test@example.invalid")
      git("config", "commit.gpgsign", "false")
      git("config", "tag.gpgsign", "false")
      git("config", "core.hooksPath", "/dev/null")
      git("commit", "--allow-empty", "-qm", "Initial public commit")
      test.call
    end
  end
  puts "PASS #{name}"
end
puts "#{cases.length} publication guard scenarios passed"
