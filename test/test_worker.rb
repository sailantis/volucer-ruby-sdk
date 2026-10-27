# frozen_string_literal: true

# The worker loop against an in-memory channel.
#
# These drive `Volucer::Worker` over StringIO pairs, which is the same thing the PHP and
# Python suites do: a real frame stream, no process. The end-to-end test is what proves the
# *other* end agrees; this one is what makes a regression legible.

require 'minitest/autorun'
require 'stringio'

require_relative '../lib/volucer'

# A server's side of the channel, in memory.
class Channel
  attr_reader :to_worker, :from_worker

  def initialize
    @to_worker = StringIO.new(+''.b)
    @from_worker = StringIO.new(+''.b)
  end

  def write(data)
    @to_worker.seek(0, IO::SEEK_END)
    @to_worker.write(data)
    self
  end

  def hello(offer_no_checksum: false)
    write(
      Volucer::Frame.new(
        Volucer::MessageType::HELLO, 0,
        Volucer.json_bytes('protocol_version' => 1, 'no_checksum' => offer_no_checksum)
      ).encode
    )
  end

  def request(id, method: 'GET', uri: '/', headers: [], body: ''.b, https: false)
    write(
      Volucer::Frame.new(
        Volucer::MessageType::REQUEST, id,
        Volucer.json_bytes(
          'method' => method, 'uri' => uri, 'protocol' => 'HTTP/1.1',
          'headers' => headers, 'remote_addr' => '127.0.0.1',
          'body_len' => body.bytesize, 'script_filename' => '/srv/app.ru',
          'https' => https
        )
      ).encode
    )
    return self if body.empty?

    write(
      Volucer::Frame.new(
        Volucer::MessageType::BODY, id, body, Volucer::Flags::LAST
      ).encode
    )
  end

  def job(id, body: ''.b, attempts: 0)
    write(
      Volucer::Frame.new(
        Volucer::MessageType::JOB, id,
        Volucer.json_bytes(
          'pipeline' => 'emails', 'id' => 'job-1', 'task' => 'send',
          'headers' => [], 'attempts' => attempts, 'body_len' => body.bytesize
        )
      ).encode
    )
    return self if body.empty?

    write(
      Volucer::Frame.new(
        Volucer::MessageType::BODY, id, body, Volucer::Flags::LAST
      ).encode
    )
  end

  def frame(type, payload = +'', id = 0)
    write(Volucer::Frame.new(type, id, payload).encode)
  end

  # Everything the worker wrote, decoded.
  def frames
    raw = @from_worker.string
    offset = 0
    out = []
    while offset < raw.bytesize
      header = Volucer::Header.decode(raw[offset, Volucer::HEADER_LEN])
      payload = raw[offset + Volucer::HEADER_LEN, header.payload_len] || ''.b
      offset += Volucer::HEADER_LEN + header.payload_len
      decoded = if [Volucer::MessageType::BODY, Volucer::MessageType::EVENT].include?(header.message_type)
                  payload
                else
                  payload.empty? ? {} : JSON.parse(payload)
                end
      out << [header, decoded]
    end
    out
  end

  def of(type)
    frames.select { |header, _| header.message_type == type }
  end
end

def worker_for(channel)
  channel.to_worker.seek(0)
  Volucer::Worker.new(channel.to_worker, channel.from_worker, StringIO.new)
end

class HandshakeTest < Minitest::Test
  def test_the_ack_reports_the_protocol_and_a_runtime
    channel = Channel.new.hello
    worker_for(channel).handshake

    ack = channel.of(Volucer::MessageType::HELLO_ACK)
    assert_equal 1, ack.length
    payload = ack.first.last
    assert_equal 1, payload['protocol_version']
    assert payload['runtime'].start_with?('volucer-worker-ruby/')
    refute payload['no_checksum']
    assert_equal Process.pid, payload['pid']
  end

  def test_the_handshake_frames_are_always_checksummed
    # Even when both ends are about to stop hashing: the agreement travels in frames, so the
    # frames carrying it must be verified.
    channel = Channel.new.hello(offer_no_checksum: true)
    worker = worker_for(channel)
    worker.handshake

    header, payload = channel.of(Volucer::MessageType::HELLO_ACK).first
    refute header.declares_no_checksum?
    assert_equal Volucer::Crc32c.digest(channel.from_worker.string[Volucer::HEADER_LEN..]),
                 header.checksum
    # And only then does the worker stop hashing.
    assert payload['no_checksum']
    assert channel.from_worker.string.bytesize.positive?
  end

  def test_a_hello_with_the_wrong_version_is_refused
    channel = Channel.new.frame(Volucer::MessageType::HELLO, '{"protocol_version":2}')
    assert_raises(Volucer::ProtocolError) { worker_for(channel).handshake }
  end

  def test_an_empty_pipe_is_not_a_handshake
    assert_raises(Volucer::ProtocolError) { worker_for(Channel.new).handshake }
  end
end

class RequestTest < Minitest::Test
  def run_one(channel)
    worker_for(channel).run { |request, _worker| yield(request) }
    channel.frames
  end

  def test_a_response_is_a_head_then_end
    channel = Channel.new.hello.request(1, uri: '/items')
    frames = run_one(channel) { |request| Volucer::Response.json('path' => request.path) }

    head = frames.filter_map { |h, p| p if h.message_type == Volucer::MessageType::RESPONSE }
    assert_equal 1, head.length
    assert_equal 200, head.first['status']
    assert_equal '{"path":"/items"}'.bytesize, head.first['body_len']

    bodies = frames.filter_map { |h, p| p if h.message_type == Volucer::MessageType::BODY }
    assert_equal ['{"path":"/items"}'], bodies

    ending = frames.filter_map { |h, p| p if h.message_type == Volucer::MessageType::END_FRAME }
    assert_equal 1, ending.length
    assert_equal 0, ending.first['app_status']
    assert ending.first['keep_alive']
    assert ending.first.key?('peak_memory_bytes')
  end

  def test_a_body_larger_than_one_chunk_is_split_and_the_last_is_flagged
    channel = Channel.new.hello.request(1)
    payload = 'x' * (Volucer::MAX_BODY_CHUNK * 2 + 17)
    frames = run_one(channel) { Volucer::Response.new(200, [], payload) }

    bodies = frames.select { |h, _| h.message_type == Volucer::MessageType::BODY }
    assert_equal 3, bodies.length
    assert_equal Volucer::MAX_BODY_CHUNK, bodies[0].last.bytesize
    assert_equal 17, bodies[2].last.bytesize
    assert_equal [false, false, true], bodies.map { |h, _| h.last? }
    assert_equal payload, bodies.map(&:last).join
  end

  def test_a_raising_block_answers_500_and_reports_the_status
    channel = Channel.new.hello.request(1)
    frames = run_one(channel) { raise 'boom' }

    head = frames.filter_map { |h, p| p if h.message_type == Volucer::MessageType::RESPONSE }
    assert_equal 500, head.first['status']
    ending = frames.filter_map { |h, p| p if h.message_type == Volucer::MessageType::END_FRAME }
    assert_equal 1, ending.first['app_status'],
                 "app_status is the application's exit status, not the HTTP status"
  end

  def test_the_request_body_is_reassembled_with_the_headers_intact
    body = 'a' * (Volucer::MAX_BODY_CHUNK + 5)
    channel = Channel.new.hello.request(
      1, method: 'POST', body: body,
      headers: [%w[Accept text/html], %w[Cookie a=1], %w[Cookie b=2]]
    )
    seen = []
    run_one(channel) do |request|
      seen << request
      Volucer::Response.text('ok')
    end

    assert_equal body, seen.first.body
    assert_equal 'text/html', seen.first.header('accept')
    assert_equal 'a=1, b=2', seen.first.header_map['Cookie'],
                 'repeated names are joined, which is what a repeated field means'
  end

  def test_https_reaches_the_request
    channel = Channel.new.hello.request(1, https: true)
    seen = []
    run_one(channel) do |request|
      seen << request
      Volucer::Response.text('ok')
    end
    assert seen.first.https?
  end

  def test_a_rack_shaped_array_becomes_a_response
    channel = Channel.new.hello.request(1)
    frames = run_one(channel) do
      [201, { 'Content-Type' => 'text/plain', 'Set-Cookie' => %w[a=1 b=2] }, ['he', 'llo']]
    end

    head = frames.filter_map { |h, p| p if h.message_type == Volucer::MessageType::RESPONSE }
    assert_equal 201, head.first['status']
    cookies = head.first['headers'].select { |name, _| name == 'Set-Cookie' }
    assert_equal [%w[Set-Cookie a=1], %w[Set-Cookie b=2]], cookies,
                 'two Set-Cookie headers are two cookies'

    bodies = frames.filter_map { |h, p| p if h.message_type == Volucer::MessageType::BODY }
    assert_equal ['hello'], bodies
  end

  def test_recycle_ends_the_loop
    channel = Channel.new.hello.request(1)
    worker_for(channel).run do |_request, worker|
      worker.recycle
      Volucer::Response.text('last one')
    end
    ending = channel.of(Volucer::MessageType::END_FRAME)
    refute ending.first.last['keep_alive']
  end

  def test_a_ping_is_answered_and_a_drain_stops_the_loop
    channel = Channel.new.hello
    channel.frame(Volucer::MessageType::PING)
    channel.frame(Volucer::MessageType::CONTROL, '{"kind":"drain"}')
    worker_for(channel).run { Volucer::Response.text('ok') }
    assert_equal 1, channel.of(Volucer::MessageType::PONG).length
    assert_equal 1, channel.of(Volucer::MessageType::CONTROL_ACK).length
  end

  def test_telemetry_from_the_server_does_not_derail_the_loop
    channel = Channel.new.hello
    channel.frame(Volucer::MessageType::LOG, '{"level":"info","message":"x"}')
    channel.request(1, uri: '/after')
    seen = []
    run_one(channel) do |request|
      seen << request.uri
      Volucer::Response.text('ok')
    end
    assert_equal ['/after'], seen
  end
end

class CallTest < Minitest::Test
  def test_a_call_is_answered_while_a_request_is_in_flight
    channel = Channel.new.hello.request(1)
    channel.frame(
      Volucer::MessageType::RETURN,
      Volucer.json_bytes('ok' => true, 'result' => { 'hit' => 1 }),
      0xFFFFFFFF
    )

    seen = []
    worker_for(channel).run do |_request, worker|
      seen << worker.call('kv.get', 'key' => 'a')
      Volucer::Response.text('ok')
    end

    assert_equal [{ 'hit' => 1 }], seen
    sent = channel.of(Volucer::MessageType::CALL).map(&:last)
    assert_equal 1, sent.length
    assert_equal 'kv.get', sent.first['name']
  end

  def test_a_call_outside_a_request_raises_rather_than_hanging
    worker_for(Channel.new).tap do |worker|
      worker.instance_variable_set(:@request_id, Volucer::UNSOLICITED)
      assert_raises(Volucer::ProtocolError) { worker.call('kv.get') }
    end
  end

  def test_a_refused_call_says_whether_the_effect_is_known
    channel = Channel.new.hello.request(1)
    channel.frame(
      Volucer::MessageType::RETURN,
      Volucer.json_bytes('ok' => false, 'error' => 'no such key', 'unknown' => true),
      0xFFFFFFFF
    )

    worker = worker_for(channel)
    errors = []
    worker.run do |_request, w|
      begin
        w.call('lock.acquire', 'name' => 'x')
      rescue Volucer::ProtocolError => e
        errors << e
      end
      Volucer::Response.text('ok')
    end

    assert_equal 1, errors.length
    refute errors.first.fatal, 'a refused call leaves the stream aligned'
    refute worker.last_call_was_known,
           'the reply said the effect may have happened, so a retry could duplicate it'
  end

  def test_a_call_with_no_handler_is_refused_by_name
    channel = Channel.new.hello
    channel.frame(Volucer::MessageType::CALL, '{"name":"kv.get","params":{}}', 0xFFFFFFF0)
    worker_for(channel).run { Volucer::Response.text('ok') }

    returns = channel.of(Volucer::MessageType::RETURN).map(&:last)
    assert_equal 1, returns.length
    refute returns.first['ok']
    assert_includes returns.first['error'], 'kv.get'
  end
end

class JobTest < Minitest::Test
  def run_job(channel)
    worker_for(channel).consume do |job, _worker|
      yield(job)
    end
    ack = channel.of(Volucer::MessageType::ACK)
    assert_equal 1, ack.length
    ack.first.last
  end

  def test_nil_acknowledges
    ack = run_job(Channel.new.hello.job(1)) { nil }
    assert_equal 'ack', ack['outcome']
    assert ack.key?('peak_memory_bytes')
    assert ack.key?('duration_us')
  end

  def test_false_requeues
    assert_equal 'requeue', run_job(Channel.new.hello.job(1)) { false }['outcome']
  end

  def test_a_string_rejects_with_a_reason
    ack = run_job(Channel.new.hello.job(1)) { 'bad payload' }
    assert_equal 'reject', ack['outcome']
    assert_equal 'bad payload', ack['reason']
  end

  def test_a_raising_block_requeues_because_losing_work_is_worse
    ack = run_job(Channel.new.hello.job(1)) { raise 'database unreachable' }
    assert_equal 'requeue', ack['outcome'],
                 'a crashing consumer must not lose the job; dropping one has to be deliberate'
  end

  def test_the_job_body_and_attempts_reach_the_block
    seen = []
    run_job(Channel.new.hello.job(1, body: 'payload', attempts: 2)) do |job|
      seen << [job.body, job.attempts]
      nil
    end
    assert_equal [['payload', 2]], seen
  end

  def test_a_job_sent_to_a_request_worker_is_requeued_rather_than_dropped
    channel = Channel.new.hello.job(1, body: 'payload')
    worker_for(channel).run { Volucer::Response.text('ok') }

    ack = channel.of(Volucer::MessageType::ACK).map(&:last)
    assert_equal 1, ack.length
    assert_equal 'requeue', ack.first['outcome']
    assert_includes ack.first['reason'], 'serves requests'
  end
end

class TransportTest < Minitest::Test
  def test_an_empty_socket_variable_means_stdio
    [nil, '', '   '].each do |value|
      if value.nil?
        ENV.delete(Volucer::SOCKET_ENV)
      else
        ENV[Volucer::SOCKET_ENV] = value
      end
      assert_nil Volucer.socket_address, "#{value.inspect} must mean stdio"
    end

    ENV[Volucer::SOCKET_ENV] = '127.0.0.1:9999'
    assert_equal '127.0.0.1:9999', Volucer.socket_address
  ensure
    ENV.delete(Volucer::SOCKET_ENV)
  end

  # The whole of the transport decision on the worker side.
  #
  # `connect` branches on `Volucer.unix_path` and nothing else, so the interesting cases are
  # about the string -- a prefix absent, present, or present with nothing after it -- and they
  # need no socket for the same reason `socket_address` does not.
  #
  # `"unix:"` with no path returns `""` rather than nil on purpose: nil means "use stdio", and
  # a server that sent `unix:` sent a broken address, not an instruction to fall back. The
  # connection then fails and says so on stderr, which on the socket transport is the only
  # channel a worker has.
  def test_a_unix_address_is_a_path_and_a_tcp_address_is_not
    cases = {
      'unix:/run/volucer/worker.0.sock' => '/run/volucer/worker.0.sock',
      '127.0.0.1:18400' => nil,
      'localhost:9000' => nil,
      'unix:' => ''
    }
    cases.each do |given, want|
      got = Volucer.unix_path(given)
      if want.nil?
        assert_nil got, "unix_path(#{given.inspect})"
      else
        assert_equal want, got, "unix_path(#{given.inspect})"
      end
    end
  end
end

class RelayResolutionTest < Minitest::Test
  # Where the supervisor's relay address comes from. No fork and no socket, so this runs on every
  # platform -- which matters, because the rule it covers is the one a deployment relies on.
  def setup
    @previous = ENV.fetch(Volucer::RELAY_ENV, nil)
    ENV.delete(Volucer::RELAY_ENV)
  end

  def teardown
    if @previous.nil?
      ENV.delete(Volucer::RELAY_ENV)
    else
      ENV[Volucer::RELAY_ENV] = @previous
    end
  end

  def test_the_argument_wins_over_the_environment
    ENV[Volucer::RELAY_ENV] = 'unix:/from-env.sock'
    assert_equal 'unix:/from-arg.sock', Volucer::Supervisor.relay('unix:/from-arg.sock')
  end

  def test_the_environment_is_the_fallback
    ENV[Volucer::RELAY_ENV] = '  unix:/from-env.sock  '
    assert_equal 'unix:/from-env.sock', Volucer::Supervisor.relay(nil)
    assert_equal 'unix:/from-env.sock', Volucer::Supervisor.relay('   '),
                 'a blank argument is absent, not an address'
  end

  def test_neither_place_set_is_nil
    assert_nil Volucer::Supervisor.relay(nil)
  end

  def test_an_empty_value_means_unset
    # `Environment=VOLUCER_RELAY_SOCKET=` in a unit file and a shell's
    # `export VOLUCER_RELAY_SOCKET=` both produce this, and reporting it as an empty address would
    # send the operator looking for a typo in a file that never mentions it.
    ENV[Volucer::RELAY_ENV] = '   '
    assert_nil Volucer::Supervisor.relay(nil)
  end
end

# `Volucer.process_rss_bytes` is what a `memory_limit_mb` pool recycles on, and it fails in a
# way that no other test can notice: it returns 0 rather than raising, and 0 is the protocol's
# legal "this runtime does not report it". So a broken implementation is indistinguishable from
# a platform that cannot measure memory, and it stayed broken through several releases.
class ProcessRssTest < Minitest::Test
  def test_a_platform_that_reports_memory_reports_a_plausible_number
    rss = Volucer.process_rss_bytes

    skip 'this platform does not report RSS' if rss.zero?

    # A Ruby process is never smaller than a megabyte, and a test process is not a terabyte.
    # The bounds exist to catch a *wrong* number -- a pointer read as a size, or the wrong
    # field of PROCESS_MEMORY_COUNTERS -- rather than to pin the actual footprint.
    assert_operator rss, :>, 1024 * 1024, "RSS of #{rss} bytes is implausibly small"
    assert_operator rss, :<, 1024**4, "RSS of #{rss} bytes is implausibly large"
  end

  # The two ways this method has actually broken. Neither raised, because the method ends in
  # `rescue StandardError => 0` -- so the assertion has to be that the *number* is right, and
  # that means not returning 0 on a platform known to report.
  def test_windows_and_linux_both_answer_rather_than_degrading_silently
    platform_reports = Gem.win_platform? || File.exist?('/proc/self/statm')
    skip 'no known RSS source on this platform' unless platform_reports

    refute_equal 0, Volucer.process_rss_bytes,
                 'this platform has an RSS source, so 0 means the reader is broken'
  end
end