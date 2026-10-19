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

  # One HTTP request, parsed. Shared rather than duplicated, because there are now two
  # server classes below and the second one started out without it -- which failed as
  # "the server never answered", because the readiness loop swallowed the `NoMethodError`.
  def self.http_get(port, path)
    socket = TCPSocket.new('127.0.0.1', port)
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
    EndToEnd.http_get(@port, path)
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

ATTACH_SUPERVISOR = <<~'RUBY'
  # An `attach` supervisor for the platforms without `fork`, using a thread.
  #
  # The SDK's real supervisor is `Volucer::Supervisor`, covered by
  # `SupervisorEndToEndTest`. This fixture exists so the *contract* -- serving a connection
  # somebody else opened -- is proven on Windows too, where `Process.fork` does not exist.
  #
  # volucer only dials the relay in this mode and creates no process at all, because the side
  # that owns the listening socket has to be the side that owns the fork.

  $LOAD_PATH.unshift(ARGV[3])
  require 'volucer'

  port = Integer(ARGV[0])
  log = ARGV[1]
  children = ARGV[2] == 'fork' && Process.respond_to?(:fork)

  note = lambda do |line|
    File.open(log, 'a') { |handle| handle.puts(line) }
  end

  serve = lambda do |connection|
    begin
      # No wrapper needed: a `TCPSocket` already responds to `read` and `write`, which is
      # what the SDK calls. Python needs `makefile` here and Ruby does not.
      Volucer::Worker.create(connection, connection).run do |request, _worker|
        Volucer::Response.text("attach #{request.uri}\n")
      end
      note.call('clean')
    rescue Exception => e # rubocop:disable Lint/RescueException
      note.call("raised #{e.class}: #{e.message}")
    ensure
      begin
        connection.close
      rescue StandardError
        nil
      end
    end
  end

  listener = TCPServer.new('127.0.0.1', port)
  note.call('ready')

  loop do
    connection = listener.accept
    note.call('accepted')

    # `fork` where the platform has it, a thread where it does not (Windows, and JRuby).
    # Both exercise the same SDK contract; only the process model differs, and that is the
    # application's decision rather than the library's.
    if children
      child = fork do
        listener.close
        serve.call(connection)
        exit!(0)
      end
      connection.close
      Process.detach(child)
    else
      Thread.new(connection) { |socket| serve.call(socket) }
    end
  end
RUBY

# The `attach` shape: volucer dials a relay somebody else listens on.
#
# Every other end-to-end test here drives `kind = "worker"`, where volucer spawns the process
# and owns its stdio -- the one case an `attach` deployment never uses. This is the test that
# proves `create` works on a connection the SDK did **not** open, which is the whole of what
# `attach` asks of an SDK.
#
# `max_requests = 2` is deliberate: it makes recycling observable inside one server run,
# without which the test would only ever exercise the first connection.
class AttachEndToEndTest < Minitest::Test
  def setup
    case EndToEnd.binary_or_report(has_binary: !EndToEnd.binary.nil?, required: EndToEnd.required?)
    when :fail
      flunk 'VOLUCER_E2E=1 but no volucer binary was found; set VOLUCER_BIN'
    when :skip
      skip 'no volucer binary: set VOLUCER_BIN or build the server checkout'
    end

    @root = Dir.mktmpdir('volucer-ruby-attach-')
    File.write(File.join(@root, 'app.rb'), "# served by the attached worker\n")
    @log = File.join(@root, 'supervisor.log')
    @port = EndToEnd.free_port
    @relay = EndToEnd.free_port

    script = File.join(@root, 'supervisor.rb')
    File.write(script, ATTACH_SUPERVISOR)
    mode = Process.respond_to?(:fork) ? 'fork' : 'thread'
    @supervisor = Process.spawn(RbConfig.ruby, script, @relay.to_s, @log, mode,
                                File.join(EndToEnd::SDK_ROOT, 'lib'),
                                out: File.join(@root, 'supervisor.out'),
                                err: File.join(@root, 'supervisor.err'))
    wait_for_log('ready', @supervisor, 'the supervisor')

    config = File.join(@root, 'volucer.toml')
    File.write(config, <<~TOML)
      default_backend = "rb"
      [server]
      http = "127.0.0.1:#{@port}"
      document_root = "#{@root.tr('\\', '/')}"
      front_controller = "app.rb"
      script_extensions = ["rb"]

      [backends.rb]
      kind = "attach"
      relay = "127.0.0.1:#{@relay}"
      workers = 1
      max_requests = 2
    TOML

    @pid = Process.spawn(EndToEnd.binary, 'serve', '--config', config,
                         out: File.join(@root, 'stdout.log'), err: File.join(@root, 'stderr.log'))
    wait_until_ready
  end

  def teardown
    # volucer first: closing its slots is how the supervisor's children are asked to stop,
    # so the other order would strand them mid-request.
    [@pid, @supervisor].compact.each do |pid|
      begin
        Process.kill(Gem.win_platform? ? 'KILL' : 'TERM', pid)
        Process.wait(pid)
      rescue Errno::ESRCH, Errno::ECHILD, Errno::EINVAL
        nil
      end
    end

    return unless @root && File.directory?(@root)

    10.times do
      begin
        FileUtils.remove_entry(@root)
        break
      rescue Errno::EACCES, Errno::ENOTEMPTY
        sleep 0.05
      end
    end
  end

  def http_get(path)
    EndToEnd.http_get(@port, path)
  end

  def wait_for_log(needle, process, what)
    deadline = Time.now + 30
    while Time.now < deadline
      return if File.file?(@log) && File.read(@log).include?(needle)

      if process && Process.waitpid(process, Process::WNOHANG)
        @supervisor = nil
        err = File.read(File.join(@root, 'supervisor.err')) rescue ''
        flunk "#{what} exited before saying #{needle.inspect}:\n#{err}"
      end
      sleep 0.05
    end
    flunk "#{what} never said #{needle.inspect}"
  end

  def wait_until_ready
    deadline = Time.now + 30
    while Time.now < deadline
      begin
        http_get('/health')
        return
      rescue StandardError
        if Process.waitpid(@pid, Process::WNOHANG)
          @pid = nil
          logs = File.read(File.join(@root, 'stderr.log')) rescue ''
          supervisor = File.read(@log) rescue ''
          flunk "the server exited before answering:\n#{logs}\nsupervisor:\n#{supervisor}"
        end
        sleep 0.05
      end
    end
    flunk 'the server never answered'
  end

  def test_a_request_reaches_the_attached_worker
    status, _, body = http_get('/hello')
    assert_equal 200, status, body
    assert_equal "attach /hello\n", body
  end

  def test_recycling_reconnects_and_the_supervisor_exits_cleanly
    # Four requests over `max_requests = 2` means two connections: requests one and two share
    # the first, three and four use the one volucer dials after closing it. A broken recycle
    # shows up as a failed third request rather than as a subtle drift.
    4.times do |index|
      status, _, body = http_get("/r#{index}")
      assert_equal 200, status, body
      assert_equal "attach /r#{index}\n", body
    end

    # And the close has to be an *end of stream*, not a reset: a worker that raised on the
    # recycle would be reported here rather than looking healthy.
    deadline = Time.now + 10
    sleep 0.05 while Time.now < deadline && !File.read(@log).include?('clean')
    lines = File.read(@log)
    refute_includes lines, 'raised', "the worker did not exit cleanly:\n#{lines}"
    assert_includes lines, 'clean', "the worker never reported a clean exit:\n#{lines}"
    assert_operator lines.scan('accepted').length, :>=, 2,
                    "recycling did not dial the relay again:\n#{lines}"
  end
end

# The SDK's own prefork supervisor, which is the half `attach` needs on Unix.
#
# Separate from `AttachEndToEndTest` because it is the *fork* under test rather than the
# plumbing around it: the fixture there uses a thread on Windows, and this class cannot. Every
# request is served by a different child, which is the property that makes `attach` cheaper
# than `worker` -- one boot, shared copy-on-write.
class SupervisorEndToEndTest < Minitest::Test
  # A supervisor that drives `Volucer::Supervisor.serve`, reporting the child that served.
  SDK_SUPERVISOR = <<~'RUBY'
    $LOAD_PATH.unshift(ARGV[2])
    require 'volucer'

    relay = ARGV[0]
    log = ARGV[1]

    handler = Volucer::Rack.handler(
      lambda do |env|
        body = "attach #{env['PATH_INFO']} pid=#{Process.pid}\n"
        [200, { 'Content-Type' => 'text/plain' }, [body]]
      end
    )

    File.open(log, 'a') { |handle| handle.puts('ready') }
    Volucer::Supervisor.serve(relay, handler, children: 2)
  RUBY

  def setup
    unless EndToEnd.binary
      flunk 'VOLUCER_E2E=1 but no volucer binary was found; set VOLUCER_BIN' if EndToEnd.required?

      skip 'no volucer binary: set VOLUCER_BIN or build the server checkout'
    end
    unless Process.respond_to?(:fork)
      skip 'the supervisor needs Process.fork(); run on Unix'
    end

    @root = Dir.mktmpdir('volucer-ruby-supervisor-')
    File.write(File.join(@root, 'app.rb'), "# served by the attached worker\n")
    @log = File.join(@root, 'supervisor.log')
    @port = EndToEnd.free_port
    @relay = EndToEnd.free_port

    script = File.join(@root, 'supervisor.rb')
    File.write(script, SDK_SUPERVISOR)
    @supervisor = Process.spawn(RbConfig.ruby, script, @relay.to_s, @log,
                                File.join(EndToEnd::SDK_ROOT, 'lib'),
                                out: File.join(@root, 'supervisor.out'),
                                err: File.join(@root, 'supervisor.err'))
    wait_for_log('ready', 'the supervisor')

    config = File.join(@root, 'volucer.toml')
    File.write(config, <<~TOML)
      default_backend = "rb"
      [server]
      http = "127.0.0.1:#{@port}"
      document_root = "#{@root.tr('\\', '/')}"
      front_controller = "app.rb"
      script_extensions = ["rb"]

      [backends.rb]
      kind = "attach"
      relay = "127.0.0.1:#{@relay}"
      workers = 2
      max_requests = 2
    TOML

    @pid = Process.spawn(EndToEnd.binary, 'serve', '--config', config,
                         out: File.join(@root, 'stdout.log'), err: File.join(@root, 'stderr.log'))
    wait_until_ready
  end

  def teardown
    [@pid, @supervisor].compact.each do |pid|
      begin
        Process.kill(Gem.win_platform? ? 'KILL' : 'TERM', pid)
        Process.wait(pid)
      rescue Errno::ESRCH, Errno::ECHILD, Errno::EINVAL
        nil
      end
    end
    return unless @root && File.directory?(@root)

    10.times do
      begin
        FileUtils.remove_entry(@root)
        break
      rescue Errno::EACCES, Errno::ENOTEMPTY
        sleep 0.05
      end
    end
  end

  def wait_for_log(needle, what)
    deadline = Time.now + 30
    while Time.now < deadline
      return if File.file?(@log) && File.read(@log).include?(needle)

      if Process.waitpid(@supervisor, Process::WNOHANG)
        @supervisor = nil
        err = File.read(File.join(@root, 'supervisor.err')) rescue ''
        flunk "#{what} exited before saying #{needle.inspect}:\n#{err}"
      end
      sleep 0.05
    end
    flunk "#{what} never said #{needle.inspect}"
  end

  def wait_until_ready
    deadline = Time.now + 30
    while Time.now < deadline
      begin
        EndToEnd.http_get(@port, '/health')
        return
      rescue StandardError
        if Process.waitpid(@pid, Process::WNOHANG)
          @pid = nil
          logs = File.read(File.join(@root, 'stderr.log')) rescue ''
          flunk "the server exited before answering:\n#{logs}"
        end
        sleep 0.05
      end
    end
    flunk 'the server never answered'
  end

  def pid_of(body)
    body.split('pid=').last.strip
  end

  def test_a_request_reaches_the_attached_worker
    status, _, body = EndToEnd.http_get(@port, '/hello')
    assert_equal 200, status, body
    assert_match(/\Aattach \/hello pid=\d+\n\z/, body)
  end

  def test_each_request_is_served_by_a_different_child
    pids = 6.times.map do |index|
      _, _, body = EndToEnd.http_get(@port, "/pid#{index}")
      pid_of(body)
    end.uniq

    # One connection per child, because volucer's `workers = 2` dials twice and each
    # `accept` forks. A pool that reused one child would show one pid, and that would mean
    # the supervisor is queueing rather than forking.
    assert_operator pids.length, :>, 1, "every request was served by the same child: #{pids}"
  end

  def test_no_child_is_left_as_a_zombie
    # The zombie is the failure mode a fork-and-forget supervisor has: one per connection,
    # invisible until the process table fills. `ps` is the only honest witness.
    6.times { |index| EndToEnd.http_get(@port, "/z#{index}") }
    sleep 1

    states = `ps -o stat= --ppid #{@supervisor}`.split
    refute_includes states, 'Z', "the supervisor left zombies behind: #{states}"
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
