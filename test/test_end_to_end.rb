# frozen_string_literal: true

# End-to-end: the real server, the real Ruby worker, the real protocol.
#
# Everything else in this suite tests this SDK against its own model of the protocol. This
# test is the one that proves the model is right, because the other end is the Rust server
# rather than a fixture.
#
# It is skipped unless a `volucer` binary is found — `VOLUCER_BIN`, or the sibling checkout.
# `VOLUCER_E2E=1` turns a skip into a failure, which is the same convention the server's own
# suites and the Python SDK use.
#
# What has to hold, and each is a way a worker can be wrong without noticing:
#
# * a request is routed to this process and the answer comes back;
# * two consecutive requests do not see each other's state;
# * application output does not corrupt the framing, because this SDK reassigns `$stdout`;
# * the server accepts the HELLO_ACK this SDK sends, including `runtime`.

require 'minitest/autorun'
require 'json'
require 'socket'
require 'tmpdir'
require 'fileutils'

module EndToEnd
  SDK_ROOT = File.expand_path('..', __dir__)
  WORKER = File.join(SDK_ROOT, 'examples', 'rack_app.rb')

  def self.binary
    explicit = ENV.fetch('VOLUCER_BIN', nil)
    return explicit if explicit && File.file?(explicit)

    [File.expand_path('../volucer/target/debug', SDK_ROOT),
     File.join(SDK_ROOT, 'volucer', 'target', 'debug')].each do |dir|
      candidate = File.join(dir, Gem.win_platform? ? 'volucer.exe' : 'volucer')
      return candidate if File.file?(candidate)
    end
    nil
  end

  #: Resolve the binary, or report why there is none.
  #
  # Mirrors `GuardTest` in the Python SDK: the skip decision is a named, testable method
  # because it was written backwards there. `binary` on its own silently falls back to the
  # sibling checkout, so a guard tested only through it cannot distinguish "found a binary"
  # from "found one where a developer happens to have a checkout".
  def self.binary_or_report(has_binary:, required:) # rubocop:disable Naming/UncommunicativeMethodParamName
    return :run if has_binary
    return :fail if required

    :skip
  end

  def self.required?
    ENV['VOLUCER_E2E'] == '1'
  end

  def self.free_port
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]
    server.close
    port
  end
end

class EndToEndTest < Minitest::Test
  def setup
    case EndToEnd.binary_or_report(has_binary: !EndToEnd.binary.nil?, required: EndToEnd.required?)
    when :fail
      flunk 'VOLUCER_E2E=1 but no volucer binary was found; set VOLUCER_BIN'
    when :skip
      skip 'no volucer binary: set VOLUCER_BIN or build the server checkout'
    end

    @root = Dir.mktmpdir('volucer-ruby-e2e-')
    # The entry point has to be routable, so the server's own `script_extensions` has to
    # name `rb` — which is step 38 of the plan, and this test is also its proof.
    File.write(File.join(@root, 'app.rb'), "# served by the worker\n")
    @port = EndToEnd.free_port

    config = File.join(@root, 'volucer.toml')
    # A TOML *literal* string for the command, because a Windows path in a basic string turns
    # `\U` into a unicode escape. A literal string cannot carry a single quote, so a command
    # containing one is rejected here rather than producing a confusing parse error.
    command = "#{RbConfig.ruby} #{EndToEnd::WORKER.tr('\\', '/')}"
    raise 'the interpreter path contains a single quote' if command.include?("'")

    File.write(config, <<~TOML)
      default_backend = "rb"
      [server]
      http = "127.0.0.1:#{@port}"
      document_root = "#{@root.tr('\\', '/')}"
      front_controller = "app.rb"
      script_extensions = ["rb"]

      [backends.rb]
      kind = "worker"
      command = '#{command}'
      workers = 1
      max_requests = 0
    TOML

    @pid = Process.spawn(EndToEnd.binary, 'serve', '--config', config,
                         out: File.join(@root, 'stdout.log'), err: File.join(@root, 'stderr.log'))
    wait_until_ready
  end

  def teardown
    return if @pid.nil?

    begin
      # `TERM` is not a signal Windows has, so the cross-platform spelling is a hard kill
      # there. On Unix the graceful signal is what lets the server stop its worker pool and
      # reap its own children, which is why it is used where it exists.
      Process.kill(Gem.win_platform? ? 'KILL' : 'TERM', @pid)
      Process.wait(@pid)
    rescue Errno::ESRCH, Errno::ECHILD, Errno::EINVAL
      nil
    end

    # Windows keeps a handle open until the child is reaped, and `remove_entry` on a file
    # another process still holds fails with EACCES — so the retry is not paranoia there.
    if @root && File.directory?(@root)
      10.times do
        FileUtils.remove_entry(@root)
        break
      rescue Errno::EACCES, Errno::ENOTEMPTY
        sleep 0.05
      end
    end
  end

  def wait_until_ready
    deadline = Time.now + 30
    while Time.now < deadline
      begin
        http_get('/health')
        return
      rescue StandardError
        if Process.waitpid(@pid, Process::WNOHANG)
          logs = File.read(File.join(@root, 'stderr.log')) rescue ''
          flunk "the server exited before answering:\n#{logs}"
        end
        sleep 0.05
      end
    end
    flunk 'the server never answered'
  end

  def http_get(path)
    socket = TCPSocket.new('127.0.0.1', @port)
    socket.write("GET #{path} HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n")
    raw = socket.read
    socket.close
    head, _, body = raw.partition("\r\n\r\n")
    status = head.lines.first.split[1].to_i
    headers = head.lines.drop(1).filter_map do |line|
      name, _, value = line.partition(':')
      [name.strip.downcase, value.strip] unless name.empty?
    end.to_h
    [status, headers, body]
  end

  def test_a_request_reaches_the_ruby_worker
    status, headers, body = http_get('/items?id=7')
    assert_equal 200, status, body
    assert_equal 'application/json', headers['content-type']
    payload = JSON.parse(body)
    assert_equal '/items', payload['path']
    assert_equal 'id=7', payload['query']
    assert_equal 'http', payload['scheme']
    assert_equal 'example.test', payload['host']
  end

  def test_two_requests_do_not_see_each_others_state
    # The acceptance criterion for a worker bridge, and the reason a request handler is
    # expected to reset: the first request's query must not appear in the second's.
    _, _, first = http_get('/first?marker=one')
    _, _, second = http_get('/second')
    assert_equal 'marker=one', JSON.parse(first)['query']
    assert_equal '', JSON.parse(second)['query'],
                 "the second request must not inherit the first one's query string"
    assert_equal '/second', JSON.parse(second)['path']
  end
end

# The skip decision, which the Python SDK shipped inverted.
#
# There, `volucer_binary() or not e2e_required()` skipped when `VOLUCER_E2E=1` was set and no
# binary existed -- so the one CI job whose purpose is proving the protocol against the real
# server proved nothing, and reported green. This SDK's `setup` had the same shape: it skipped
# on a missing binary and never consulted `required?`. These cases pin the rule rather than the
# expression: run when there is a binary, fail when a skip is not permitted, skip otherwise.
class GuardTest < Minitest::Test
  def test_runs_whenever_it_could_do_something
    assert_equal :run, EndToEnd.binary_or_report(has_binary: true, required: false)
    assert_equal :run, EndToEnd.binary_or_report(has_binary: true, required: true)
    assert_equal :fail, EndToEnd.binary_or_report(has_binary: false, required: true),
                 'a missing binary with VOLUCER_E2E=1 must fail, never skip'
  end

  def test_skips_only_when_there_is_nothing_to_do
    assert_equal :skip, EndToEnd.binary_or_report(has_binary: false, required: false)
  end

  def test_required_reads_the_variable_ci_sets
    ENV['VOLUCER_E2E'] = '1'
    assert EndToEnd.required?
    ENV['VOLUCER_E2E'] = '0'
    refute EndToEnd.required?, 'only 1 counts; 0 is not a requirement'
    ENV.delete('VOLUCER_E2E')
    refute EndToEnd.required?, 'unset is not a requirement'
  end
end
