# frozen_string_literal: true

# The `[rpc]` call plane, driven without a server process.
#
# A `Socket.pair` gives this file a real socket for the transport and a script it can drive frame by
# frame, which is exactly the shape a test of a *client* wants: every interesting case here is about
# what the client does with a particular answer, or with no answer at all, and none of them need a
# volucer process.
#
# The frame helpers are written against {Volucer::Frame} rather than a request-shaped helper, because
# a test that a call *carries the token* has to assert on the bytes, and a helper that went through a
# decoded Hash would be asserting on its own assumption.
#
# The other end of this wire is tested in Rust -- `crates/volucer-server/tests/rpc.rs` -- and the
# end-to-end suite drives a real server. What only this file can show is the client's own failures: a
# refusal, a deadline, a connection that breaks after the call was written.

require 'minitest/autorun'
require 'socket'
require 'thread'

require_relative '../lib/volucer'

class RpcTest < Minitest::Test
  # A connected pair of stream sockets: `[client, server]`.
  #
  # **The family is chosen per platform, and both choices are forced.** `AF_INET` pairs are refused on
  # Linux -- `socketpair(AF_INET, …)` fails there -- and `AF_UNIX` is not universally available
  # either. Ruby on Windows has both, so this asks for the better one and falls back; the client
  # cannot tell the difference, which is the point of testing the transport rather than the network.
  def pair
    families = Socket.const_defined?(:AF_UNIX) ? [:UNIX, :INET] : [:INET]
    families.each do |family|
      return Socket.pair(family, :STREAM)
    rescue SystemCallError, SocketError
      next
    end
    raise 'this platform supports neither a unix nor an inet socket pair'
  end

  # One frame the client wrote, decoded.
  def read_frame(socket)
    header = Volucer::Header.decode(read_exactly(socket, 16))
    payload = header.payload_len.zero? ? '' : read_exactly(socket, header.payload_len)
    {
      'type' => header.message_type,
      'id' => header.request_id,
      'payload' => payload.empty? ? {} : JSON.parse(payload),
      'raw' => payload,
      'checksum' => header.checksum
    }
  end

  def read_exactly(socket, length)
    got = +''.b
    while got.bytesize < length
      chunk = socket.read(length - got.bytesize)
      raise "the client wrote only #{got.bytesize} of #{length} bytes" if chunk.nil? || chunk.empty?

      got << chunk
    end
    got
  end

  # Answer a call on the server end of a pair.
  def write_return(socket, request_id, answer)
    payload = JSON.generate(answer)
    socket.write(Volucer::Frame.new(Volucer::MessageType::RETURN, request_id, payload).encode)
    socket.flush
  end

  # A plane bound to one end of a pair, with a short deadline.
  def plane(client, token: 'a-token', timeout: 0.5)
    Volucer::Rpc.new(client, token, '127.0.0.1:1', timeout)
  end

  # Run `body` against a scripted server and answer what the client wrote.
  #
  # `answer` is the result of the call, or a whole RETURN object when the test is about a refusal.
  def exchange(answer = nil, raw: nil, token: 'a-token')
    client, server = pair
    written = nil
    thread = Thread.new do
      written = read_frame(server)
      body = raw || (answer.is_a?(Hash) && answer.key?('ok') ? answer : { 'ok' => true, 'result' => answer })
      write_return(server, written['id'], body)
    end
    result = yield plane(client, token: token)
    thread.join(5)
    [result, written]
  ensure
    client&.close
    server&.close
  end

  # ---------------------------------------------------------------- the seam

  def test_a_plane_is_built_without_an_address
    # A backend that granted nothing is handed no address. That is an ordinary state -- a request
    # which never touches a store must still work -- so the plane refuses the *call*, with the
    # variable named, rather than raising from the constructor.
    unconfigured = Volucer::Rpc.connect(address: nil, token: nil, env: {})

    error = assert_raises(Volucer::ProtocolError) { unconfigured.call('kv.cache.get', 'key' => 'YQ==') }

    assert_includes error.message, 'VOLUCER_RPC'
    assert_includes error.message, 'services'
    refute error.unknown?, 'nothing was sent, so the call certainly did not happen'
    refute unconfigured.last_call_was_unknown?
    assert_includes unconfigured.last_call_error, 'VOLUCER_RPC'
  end

  def test_a_missing_address_names_where_it_looked
    # The message has to distinguish "the process's environment" from "the request's", because those
    # are different mistakes: a CLI script versus a backend with no `services` list.
    from_environment = Volucer::Rpc.connect(env: nil, address: nil, token: nil)
    assert_includes from_environment.last_call_error, 'its environment'

    from_rack = Volucer::Rpc.connect(env: {})
    assert_includes from_rack.last_call_error, 'Rack request environment'
  end

  def test_a_zero_timeout_is_refused
    # A deadline of zero would abandon every call, which is never what a caller meant.
    assert_raises(ArgumentError) { Volucer::Rpc.new(nil, 't', '127.0.0.1:1', 0) }
  end

  def test_the_cgi_parameters_are_read
    # Dialling is deferred to the first call, so this asserts the *reading* without a listener.
    plane = Volucer::Rpc.connect(env: { 'VOLUCER_RPC' => '127.0.0.1:1' })

    assert_equal '127.0.0.1:1', plane.address
  end

  def test_the_upstream_header_form_is_read
    # An application behind an `upstream` backend has no CGI parameters at all.
    plane = Volucer::Rpc.connect(env: { 'HTTP_X_VOLUCER_RPC' => '127.0.0.1:1' })

    assert_equal '127.0.0.1:1', plane.address
  end

  def test_a_blank_value_reads_as_absent
    # `VOLUCER_RPC=` is what a shell produces while somebody is debugging, and it is not an address.
    plane = Volucer::Rpc.connect(env: { 'VOLUCER_RPC' => '   ' })

    assert_nil plane.address
  end

  def test_the_rack_environment_is_read_when_given
    # A Rack application has to pass its `env`, because the request's environment is a different thing
    # from the process's: under FastCGI the values arrive as CGI parameters, and `ENV` is not them.
    plane = Volucer::Rpc.connect(env: { 'VOLUCER_RPC' => '127.0.0.1:6001', 'VOLUCER_RPC_TOKEN' => 'tok' })

    assert_equal '127.0.0.1:6001', plane.address
  end

  def test_an_explicit_address_beats_the_environment
    plane = Volucer::Rpc.connect(address: 'unix:/tmp/given.sock', env: { 'VOLUCER_RPC' => '127.0.0.1:1' })

    assert_equal 'unix:/tmp/given.sock', plane.address
  end

  # ------------------------------------------------------------------ calls

  def test_a_call_carries_the_token
    _result, written = exchange(nil, token: 'the-token') { |rpc| rpc.call('kv.cache.get', 'key' => 'YQ==') }

    assert_equal Volucer::MessageType::CALL, written['type']
    assert_equal 'the-token', written['payload']['token']
    assert_equal 'kv.cache.get', written['payload']['name']
  end

  def test_a_call_with_no_token_omits_the_field
    # Omitted rather than sent empty: the server tells "carried no token" from "carried a token no
    # backend was issued", and they are fixed in different files.
    _result, written = exchange(nil, token: '') { |rpc| rpc.call('kv.cache.get', {}) }

    refute written['payload'].key?('token')
  end

  def test_a_served_call_returns_the_result
    result, = exchange({ 'value' => 'aGk=' }) { |rpc| rpc.call('kv.cache.get', 'key' => 'YQ==') }

    assert_equal({ 'value' => 'aGk=' }, result)
  end

  def test_a_call_id_descends_from_the_top_of_the_range
    # Call ids and request ids are deliberately from opposite ends, so a call mistaken for a request is
    # a visible correlation failure rather than a plausible answer.
    _result, written = exchange(nil) { |rpc| rpc.call('kv.cache.get') }

    assert_equal 0xFFFFFFFF, written['id']
  end

  def test_every_frame_is_checksummed
    # There is no handshake here, so nothing negotiated the checksum away: the header carries a real
    # CRC-32C of the payload rather than the NO_CHECKSUM flag.
    _result, written = exchange(nil) { |rpc| rpc.call('kv.cache.get') }

    assert_equal Volucer::Crc32c.digest(written['raw']), written['checksum']
  end

  def test_a_refusal_raises_and_is_not_unknown
    client, server = pair
    thread = Thread.new do
      sent = read_frame(server)
      write_return(server, sent['id'], { 'ok' => false, 'error' => 'this backend may not reach the cache' })
    end
    rpc = plane(client)

    error = assert_raises(Volucer::ProtocolError) { rpc.call('kv.cache.get', {}) }
    thread.join(5)

    assert_includes error.message, 'may not reach'
    refute error.unknown?, 'the server answered a refusal, so nothing was left undecided'
    refute rpc.last_call_was_unknown?
    # The stream is aligned after a refusal, so the connection is kept for the next call.
    assert rpc.connected?
  ensure
    client&.close
    server&.close
  end

  def test_an_undecided_answer_raises_unknown
    client, server = pair
    thread = Thread.new do
      sent = read_frame(server)
      write_return(server, sent['id'], { 'ok' => false, 'unknown' => true, 'error' => 'the lock state is unknown' })
    end
    rpc = plane(client)

    error = assert_raises(Volucer::ProtocolError) { rpc.call('lock.acquire', 'resource' => 'r') }
    thread.join(5)

    assert error.unknown?, 'a refusal carrying unknown:true says the effect may have happened'
    assert error.may_have_taken_effect?, 'the same fact, in the words a caller asks it in'
    assert rpc.last_call_was_unknown?
  ensure
    client&.close
    server&.close
  end

  def test_a_deadline_expires_rather_than_hanging
    # A socket that accepts and never answers, which is what "the server has stopped reading" looks
    # like from here. The read deadline is the only thing that ends it.
    client, server = pair
    rpc = plane(client, timeout: 0.2)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)

    error = assert_raises(Volucer::ProtocolError) { rpc.call('kv.cache.get', {}) }
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_operator elapsed, :<, 3.0, 'the deadline has to bound the wait'
    assert error.unknown?, 'a call that was written and not answered may have been served'
    assert_includes error.message, 'did not answer'
    refute rpc.connected?, 'an undecided call discards the connection'
  ensure
    client&.close
    server&.close
  end

  def test_a_closed_connection_is_reported_as_undecided
    # Not asserted on the *wording*: Windows answers a read on a closed socket with WSAECONNRESET
    # ("An existing connection was forcibly closed by the remote host") where POSIX answers a clean
    # EOF, so a message test here pins the platform rather than the behaviour. What must hold
    # everywhere is that the call was written, the answer never arrived, and the socket is gone.
    client, server = pair
    rpc = plane(client)
    server.close

    error = assert_raises(Volucer::ProtocolError) { rpc.call('kv.cache.get', {}) }

    assert error.unknown?, 'the call was written and never answered, so it may have been served'
    refute rpc.connected?, 'a broken connection is discarded rather than reused'
    refute_nil rpc.last_call_error
  ensure
    client&.close
  end

  def test_a_frame_that_is_not_a_return_is_refused
    client, server = pair
    thread = Thread.new do
      sent = read_frame(server)
      server.write(Volucer::Frame.new(Volucer::MessageType::LOG, sent['id'], '{"message":"hi"}').encode)
      server.flush
    end
    rpc = plane(client)

    error = assert_raises(Volucer::ProtocolError) { rpc.call('kv.cache.get', {}) }
    thread.join(5)

    assert_includes error.message, 'LOG'
    assert error.unknown?
  ensure
    client&.close
    server&.close
  end

  def test_a_corrupt_checksum_discards_the_connection
    client, server = pair
    thread = Thread.new do
      sent = read_frame(server)
      frame = Volucer::Frame.new(Volucer::MessageType::RETURN, sent['id'], '{"ok":true,"result":null}').encode
      # The checksum is the last four bytes of the header, little-endian. Flipping one leaves a frame
      # whose length is right and whose contents are not -- the case a reader must not accept.
      frame.setbyte(12, frame.getbyte(12) ^ 0xFF)
      server.write(frame)
      server.flush
    end
    rpc = plane(client)

    error = assert_raises(Volucer::ProtocolError) { rpc.call('kv.cache.get', {}) }
    thread.join(5)

    assert error.unknown?
    assert_includes error.message, 'checksum that does not match'
    refute rpc.connected?, 'a desynchronised stream is discarded rather than reused'
  ensure
    client&.close
    server&.close
  end

  def test_a_truncated_frame_ends_at_the_deadline
    # A header that declares more payload than the peer sends is indistinguishable from a slow peer
    # until the deadline expires -- which is exactly why this transport has one.
    client, server = pair
    thread = Thread.new do
      read_frame(server)
      server.write(Volucer::Frame.new(Volucer::MessageType::RETURN, 0xFFFFFFFF, 'x' * 64).encode[0, 20])
      server.flush
    end
    rpc = plane(client, timeout: 0.2)

    error = assert_raises(Volucer::ProtocolError) { rpc.call('kv.cache.get', {}) }
    thread.join(5)

    assert error.unknown?
    assert_includes error.message, 'did not answer within'
  ensure
    client&.close
    server&.close
  end

  def test_a_return_for_another_call_is_refused
    client, server = pair
    thread = Thread.new do
      read_frame(server)
      write_return(server, 1234, { 'ok' => true, 'result' => nil })
    end
    rpc = plane(client)

    error = assert_raises(Volucer::ProtocolError) { rpc.call('kv.cache.get', {}) }
    thread.join(5)

    assert_includes error.message, 'arrived while waiting for call'
  ensure
    client&.close
    server&.close
  end

  # -------------------------------------------------------------- addresses

  def test_a_unix_address_becomes_a_path
    path, tcp = Volucer::Rpc.address_of('unix:/run/volucer/rpc.sock')

    assert_equal '/run/volucer/rpc.sock', path
    assert_nil tcp
  end

  def test_a_tcp_address_becomes_a_host_and_port
    path, tcp = Volucer::Rpc.address_of('127.0.0.1:6001')

    assert_nil path
    assert_equal ['127.0.0.1', 6001], tcp
  end

  def test_a_bare_port_means_loopback
    _path, tcp = Volucer::Rpc.address_of(':6001')

    assert_equal ['127.0.0.1', 6001], tcp
  end

  def test_a_malformed_address_is_refused
    assert_raises(ArgumentError) { Volucer::Rpc.address_of('not an address') }
  end

  # -------------------------------------------------------------- round trip

  def test_a_real_socket_round_trip
    # The pair above is the transport; this drives `connect` through an actual listener, so the dial
    # and the address parsing are exercised together rather than only the framing.
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]
    thread = Thread.new do
      socket = server.accept
      sent = read_frame(socket)
      write_return(socket, sent['id'], { 'ok' => true, 'result' => { 'value' => 'aGk=' } })
      socket.close
    end

    rpc = Volucer::Rpc.connect(address: "127.0.0.1:#{port}", token: 'from-a-listen')

    assert_equal({ 'value' => 'aGk=' }, rpc.call('kv.cache.get', 'key' => Volucer::Kv.key_of('greeting')))
    assert rpc.connected?
    thread.join(5)
  ensure
    rpc&.close
    server&.close if server && !server.closed?
  end

  def test_a_call_after_a_broken_connection_dials_again
    # The behaviour a plane reused across two calls depends on: a call whose socket died discards it,
    # and the next call dials a fresh one. `ensure_stream` is the single path both cases go through.
    #
    # The first connection is closed *without* answering, using `shutdown` rather than `close` so the
    # client reads a clean end of stream: closing a socket with an unread answer still in flight makes
    # Windows send an RST, which is a different (and noisier) path to the same conclusion.
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]
    thread = Thread.new do
      first = server.accept
      read_frame(first)
      first.shutdown(Socket::SHUT_WR)
      first.close

      second = server.accept
      frame = read_frame(second)
      write_return(second, frame['id'], { 'ok' => true, 'result' => { 'reconnected' => true } })
      second.close
    end

    rpc = Volucer::Rpc.connect(address: "127.0.0.1:#{port}", token: 't', timeout: 2.0)

    error = assert_raises(Volucer::ProtocolError) { rpc.call('kv.cache.get', {}) }
    assert error.unknown?, 'the first call was written and never answered'
    refute rpc.connected?, 'the dead socket is discarded'

    result = rpc.call('kv.cache.get', {})

    assert_equal({ 'reconnected' => true }, result)
    thread.join(5)
  ensure
    rpc&.close
    server&.close if server && !server.closed?
  end

  def test_an_unreachable_address_fails_the_call_not_the_constructor
    # The port is bound and immediately released, so nothing is listening.
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]
    server.close

    rpc = Volucer::Rpc.connect(address: "127.0.0.1:#{port}", token: 't', timeout: 0.5)

    error = assert_raises(Volucer::ProtocolError) { rpc.call('kv.cache.get', {}) }

    assert_includes error.message, 'could not be reached'
    refute error.unknown?, 'nothing was sent, so the call certainly did not happen'
  end
end
