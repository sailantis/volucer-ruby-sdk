# frozen_string_literal: true

# The three namespaces, asserted on the bytes they write.
#
# Every method here has a field name and an encoding the server chose, and none of them is guessable
# from the method's Ruby name: a KV key is base64, a lock's `resource` is not, a KV `ttl` is seconds
# and a lock's is microseconds, and the answer to `exists` is `exists` rather than `held`. Those are
# exactly the things a client gets wrong once and then never notices, because the wrong field name
# reads as `false` and the wrong unit reads as a lease of a month.
#
# The server's half of this is `a_batch_key_is_base64_in_both_directions` and its neighbours in
# `crates/volucer-server/src/calls.rs`. The batch methods are pinned on both sides because the two
# implementations disagreed about them for three releases with no test on either side.

require 'minitest/autorun'
require 'socket'
require 'thread'

require_relative '../lib/volucer'

class ServicesTest < Minitest::Test
  # A base64 of a string, so an expectation reads as the value it carries.
  def b64(text)
    Volucer::Base64Codec.encode(text)
  end

  def pair
    families = Socket.const_defined?(:AF_UNIX) ? [:UNIX, :INET] : [:INET]
    families.each do |family|
      return Socket.pair(family, :STREAM)
    rescue SystemCallError, SocketError
      next
    end
    raise 'this platform supports neither a unix nor an inet socket pair'
  end

  def read_frame(socket)
    header = Volucer::Header.decode(read_exactly(socket, 16))
    payload = header.payload_len.zero? ? '' : read_exactly(socket, header.payload_len)
    { 'type' => header.message_type, 'id' => header.request_id, 'payload' => JSON.parse(payload) }
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

  def write_return(socket, request_id, answer)
    socket.write(Volucer::Frame.new(Volucer::MessageType::RETURN, request_id, JSON.generate(answer)).encode)
    socket.flush
  end

  # Run `body` against a server that answers each call in order, and answer what the client wrote.
  #
  # `answers` are whole RETURN objects, so a test about a refusal writes the refusal rather than
  # relying on the helper to guess.
  def converse(*answers)
    client, server = pair
    written = []
    thread = Thread.new do
      answers.each do |answer|
        frame = read_frame(server)
        written << frame
        write_return(server, frame['id'], answer)
      end
    end
    rpc = Volucer::Rpc.new(client, 'a-token', '127.0.0.1:1', 5.0)
    result = yield rpc
    thread.join(5)
    [result, written]
  ensure
    client&.close
    server&.close
  end

  # A successful answer carrying `result`.
  def ok(result)
    { 'ok' => true, 'result' => result }
  end

  # ------------------------------------------------------------ kv: addresses

  def test_a_kv_call_names_its_storage_and_verb
    # Three parts, storage in the middle. A client that built `kv.get` would be refused with the list
    # of storages it may reach, which is the message an operator needs.
    _result, written = converse(ok({ 'value' => nil })) { |rpc| rpc.kv('cache').get('greeting') }

    assert_equal 'kv.cache.get', written[0]['payload']['name']
  end

  def test_the_default_storage_is_named_default
    _result, written = converse(ok({ 'value' => nil })) { |rpc| rpc.kv.get('k') }

    assert_equal 'kv.default.get', written[0]['payload']['name']
  end

  def test_a_key_is_base64
    _result, written = converse(ok({ 'value' => nil })) { |rpc| rpc.kv('cache').get('greeting') }

    assert_equal b64('greeting'), written[0]['payload']['params']['key']
  end

  # ------------------------------------------------------------ kv: writing

  def test_a_value_is_json_then_base64
    _result, written = converse(ok({ 'stored' => true })) { |rpc| rpc.kv('cache').set('k', { 'a' => 1 }) }

    assert_equal b64('{"a":1}'), written[0]['payload']['params']['value']
  end

  def test_a_bytes_value_is_not_re_encoded
    # A caller who compressed or encrypted the value should not have it wrapped in a JSON string,
    # which would double its size and change what the consumer reads.
    _result, written = converse(ok({ 'stored' => true })) { |rpc| rpc.kv('cache').set('k', "\x00\xff".b) }

    assert_equal b64("\x00\xff".b), written[0]['payload']['params']['value']
  end

  def test_a_utf8_string_is_still_json
    # The counterpart of the test above, and the reason `written` tests the *encoding* rather than the
    # class: a plain String is what a caller writes most of the time, and it must round-trip as JSON
    # so that the same value read back is a String rather than a quoted one.
    _result, written = converse(ok({ 'stored' => true })) { |rpc| rpc.kv('cache').set('k', 'hello') }

    assert_equal b64('"hello"'), written[0]['payload']['params']['value']
  end

  def test_a_string_that_is_not_valid_utf8_is_treated_as_bytes
    # `File.read` of a binary file gives exactly this: a String tagged UTF-8 whose bytes are not.
    # Ruby permits it and the JSON encoder refuses with `JSON::GeneratorError` from inside the
    # library, so the client has to decide -- and a String that cannot be JSON text *is* bytes.
    data = "\xC3\x28".dup.force_encoding(Encoding::UTF_8)
    refute data.valid_encoding?

    _result, written = converse(ok({ 'stored' => true })) { |rpc| rpc.kv('cache').set('k', data) }

    assert_equal b64(data.b), written[0]['payload']['params']['value']
  end

  def test_a_kv_ttl_is_seconds
    _result, written = converse(ok({ 'stored' => true })) { |rpc| rpc.kv('cache').set('k', 'v', ttl: 60) }

    assert_equal 60, written[0]['payload']['params']['ttl']
  end

  def test_no_ttl_sends_no_ttl
    # Absent rather than `null`: the server reads absent as "no expiry", `0` as "delete this", and a
    # client that sent `null` for the first would be sending a third thing to decode.
    _result, written = converse(ok({ 'stored' => true })) { |rpc| rpc.kv('cache').set('k', 'v') }

    refute written[0]['payload']['params'].key?('ttl')
  end

  def test_a_zero_ttl_is_sent_rather_than_omitted
    # `ttl: 0` means "delete this", which is not the same as "no expiry" -- so it has to survive.
    _result, written = converse(ok({ 'stored' => true })) { |rpc| rpc.kv('cache').set('k', 'v', ttl: 0) }

    assert_equal 0, written[0]['payload']['params']['ttl']
  end

  def test_set_if_absent_is_its_own_verb
    result, written = converse(ok({ 'stored' => false })) { |rpc| rpc.kv('cache').set_if_absent('k', 'v') }

    assert_equal 'kv.cache.setIfAbsent', written[0]['payload']['name']
    refute result, 'somebody else got there first, which is a successful answer'
  end

  # ------------------------------------------------------------ kv: batches

  def test_a_batch_key_is_base64_like_a_single_one
    # The bug this pins: the server read the list as literal strings while decoding every scalar key,
    # so a batch read reported every key as missing.
    _result, written = converse(ok({ 'values' => [] })) do |rpc|
      rpc.kv('cache').get_multiple(['greeting'])
    end

    assert_equal [b64('greeting')], written[0]['payload']['params']['keys']
  end

  def test_a_batch_write_encodes_both_halves
    _result, written = converse(ok({ 'stored' => 1 })) do |rpc|
      rpc.kv('cache').set_multiple({ 'farewell' => 'bye' })
    end

    assert_equal(
      { b64('farewell') => b64('"bye"') },
      written[0]['payload']['params']['values'],
      'the key is base64 and the value is base64 of its JSON encoding'
    )
  end

  def test_a_batch_read_keeps_positions_and_omits_misses
    _result, written = converse(ok({ 'values' => [b64('"a"'), nil, b64('"c"')] })) do |rpc|
      rpc.kv('cache').get_many(%w[first second third])
    end

    # Every key is encoded, including the ones the answer omitted: the server matches by position, so
    # a client that dropped a key from the request would shift every value by one.
    assert_equal %w[first second third],
                 written[0]['payload']['params']['keys'].map { |key| Volucer::Base64Codec.decode(key) }
  end

  def test_get_multiple_omits_a_miss_and_get_many_keeps_the_hole
    # The two methods exist because the answers differ, and a caller that needs the positions cannot
    # recover them from a Hash with the misses dropped.
    found, = converse(ok({ 'values' => [b64('"a"'), nil] })) do |rpc|
      rpc.kv('cache').get_multiple(%w[first second])
    end
    assert_equal({ 'first' => 'a' }, found)

    positional, = converse(ok({ 'values' => [b64('"a"'), nil] })) do |rpc|
      rpc.kv('cache').get_many(%w[first second])
    end
    assert_equal ['a', nil], positional
  end

  def test_a_short_values_list_reads_as_misses_rather_than_raising
    # A server that answered fewer values than keys is not a frame error, and a client that raised
    # would turn a partial answer into a broken cache.
    found, = converse(ok({ 'values' => [b64('"a"')] })) do |rpc|
      rpc.kv('cache').get_multiple(%w[first second])
    end

    assert_equal({ 'first' => 'a' }, found)
  end

  # -------------------------------------------------------------- kv: reading

  def test_a_miss_is_nil_and_not_a_failure
    # The distinction a cache must not collapse: `kv.get` on a missing key *succeeded*.
    result, = converse(ok({ 'value' => nil })) { |rpc| rpc.kv('cache').get('k') }

    assert_nil result
  end

  def test_a_stored_value_comes_back_decoded
    result, = converse(ok({ 'value' => b64('{"a":1}') })) { |rpc| rpc.kv('cache').get('k') }

    assert_equal({ 'a' => 1 }, result)
  end

  def test_a_value_that_is_not_json_comes_back_as_bytes
    # A value written by another language through the same storage need not be JSON, and returning
    # the bytes is the honest answer rather than an exception that makes one storage unusable by two.
    result, = converse(ok({ 'value' => b64("\x00\xff") })) { |rpc| rpc.kv('cache').get('k') }

    assert_equal "\x00\xff".b, result
  end

  def test_a_server_value_that_is_not_base64_is_a_protocol_error
    error = assert_raises(Volucer::ProtocolError) do
      converse(ok({ 'value' => 'not base64!' })) { |rpc| rpc.kv('cache').get('k') }
    end

    assert_includes error.message, 'not valid base64'
  end

  def test_has_reads_the_present_field
    result, written = converse(ok({ 'present' => true })) { |rpc| rpc.kv('cache').has?('k') }

    assert_equal 'kv.cache.has', written[0]['payload']['name']
    assert result
  end

  def test_delete_reads_the_deleted_field
    result, = converse(ok({ 'deleted' => false })) { |rpc| rpc.kv('cache').delete('k') }

    refute result, 'nothing was there, which is an answer'
  end

  def test_ttl_is_a_word_or_a_number
    forever, = converse(ok({ 'ttl' => 'forever' })) { |rpc| rpc.kv('cache').ttl('k') }
    missing, = converse(ok({ 'ttl' => 'missing' })) { |rpc| rpc.kv('cache').ttl('k') }
    seconds, = converse(ok({ 'ttl' => 42 })) { |rpc| rpc.kv('cache').ttl('k') }

    assert_equal 'forever', forever
    assert_equal 'missing', missing
    assert_equal 42, seconds
  end

  def test_clear_names_the_verb_and_reads_cleared
    result, written = converse(ok({ 'cleared' => true })) { |rpc| rpc.kv('cache').clear }

    assert_equal 'kv.cache.clear', written[0]['payload']['name']
    assert result
  end

  def test_a_refusal_names_the_storage
    # "kv.get failed" does not say which store, and the server's refusal already names the ones this
    # backend may reach -- so the client's message must not bury it.
    error = assert_raises(Volucer::ProtocolError) do
      converse({ 'ok' => false, 'error' => 'this backend may not reach "cache"; it may reach "default"' }) do |rpc|
        rpc.kv('cache').get('k')
      end
    end

    assert_includes error.message, 'kv.cache.get failed'
    assert_includes error.message, 'may reach "default"'
  end

  # ------------------------------------------------------------ kv: local rules

  def test_a_non_string_key_is_refused_here
    assert_raises(TypeError) { Volucer::Kv.key_of(1) }
  end

  def test_an_empty_key_is_refused_here
    assert_raises(ArgumentError) { Volucer::Kv.key_of('') }
  end

  def test_a_binary_key_is_used_as_given
    # A caller who hashes or encrypts keys has bytes, and encoding them as text would be a silent
    # difference from the same key on another transport.
    assert_equal b64("\x00\xff".b), Volucer::Kv.key_of("\x00\xff".b)
  end

  # ------------------------------------------------------------------ locks

  def test_an_acquire_carries_microseconds_and_defaults_to_exclusive
    result, written = converse(ok({ 'acquired' => true, 'id' => 'holder-1' })) do |rpc|
      rpc.lock.acquire('pdf:create', ttl: 5_000)
    end

    params = written[0]['payload']['params']
    assert_equal 'lock.acquire', written[0]['payload']['name']
    assert_equal 5_000, params['ttl'], 'a lock lease is microseconds, unlike a KV ttl'
    assert_equal 0, params['wait']
    assert_equal 'exclusive', params['mode'], 'a shared lock would silently stop two writers excluding each other'
    assert_equal 'pdf:create', params['resource'], 'a resource is a name, so it is plain text'
    assert_equal 'holder-1', result
  end

  def test_a_shared_acquire_is_spelled_shared
    _result, written = converse(ok({ 'acquired' => true, 'id' => 'h' })) do |rpc|
      rpc.lock.acquire('r', mode: :read)
    end

    assert_equal 'shared', written[0]['payload']['params']['mode']
  end

  def test_contention_is_nil_rather_than_an_error
    result, = converse(ok({ 'acquired' => false, 'id' => 'generated' })) { |rpc| rpc.lock.acquire('r') }

    assert_nil result, 'nothing failed, the caller just knows it is not in'
  end

  def test_try_acquire_is_its_own_verb
    _result, written = converse(ok({ 'acquired' => false })) { |rpc| rpc.lock.try_acquire('r') }

    assert_equal 'lock.try_acquire', written[0]['payload']['name']
  end

  def test_exists_reads_the_field_the_server_sends
    # Guessed wrong once while writing the Python client: the answer is `exists`, not `held`.
    result, = converse(ok({ 'exists' => true })) { |rpc| rpc.lock.exists?('r') }

    assert result
  end

  def test_release_reads_released
    result, written = converse(ok({ 'released' => true, 'id' => 'h' })) { |rpc| rpc.lock.release('r', 'h') }

    assert_equal 'lock.release', written[0]['payload']['name']
    assert result
  end

  def test_force_release_is_release_all_on_the_wire
    result, written = converse(ok({ 'released' => true })) { |rpc| rpc.lock.force_release('r') }

    assert_equal 'lock.release_all', written[0]['payload']['name']
    assert result
  end

  def test_holders_is_a_list_of_hashes
    holders = [{ 'id' => 'h', 'mode' => 'shared', 'remaining_ms' => 1_000 }]
    result, written = converse(ok({ 'holders' => holders })) { |rpc| rpc.lock.holders('r') }

    assert_equal 'lock.holders', written[0]['payload']['name']
    assert_equal holders, result
  end

  def test_update_ttl_reads_updated
    result, written = converse(ok({ 'updated' => false, 'id' => 'h' })) { |rpc| rpc.lock.update_ttl('r', 'h', 1_000) }

    assert_equal 'lock.update_ttl', written[0]['payload']['name']
    refute result, 'the id held nothing, which is the answer to act on'
  end

  def test_a_bad_mode_is_refused_here
    assert_raises(ArgumentError) { Volucer::Lock.mode_of(:exclusiveish) }
  end

  # ------------------------------------------------------------------- jobs

  def test_a_push_carries_its_task_and_payload
    result, written = converse(ok({ 'pushed' => true, 'id' => 'job-1' })) do |rpc|
      rpc.jobs('mail').push('SendEmail', payload: '{"to":"a@b"}')
    end

    params = written[0]['payload']['params']
    assert_equal 'jobs.push', written[0]['payload']['name']
    assert_equal 'mail', params['pipeline']
    assert_equal 'SendEmail', params['task']
    assert_equal b64('{"to":"a@b"}'), params['payload']
    assert_equal 'job-1', result
  end

  def test_a_hash_payload_is_json_encoded
    _result, written = converse(ok({ 'pushed' => true, 'id' => 'job-1' })) do |rpc|
      rpc.jobs('mail').push('SendEmail', payload: { 'to' => 'a@b' })
    end

    assert_equal b64('{"to":"a@b"}'), written[0]['payload']['params']['payload']
  end

  def test_a_push_without_a_task_is_refused_before_it_is_sent
    # Refused here rather than by the server, because the server's refusal would be a round trip on a
    # call that was never going to work -- and the message names the caller's line.
    client, server = pair
    rpc = Volucer::Rpc.new(client, 't', '127.0.0.1:1', 1.0)

    assert_raises(ArgumentError) { rpc.jobs('mail').push('') }
    assert_raises(ArgumentError) { rpc.jobs('mail').push(nil) }
  ensure
    client&.close
    server&.close
  end

  def test_a_non_string_header_is_refused_here
    client, server = pair
    rpc = Volucer::Rpc.new(client, 't', '127.0.0.1:1', 1.0)

    assert_raises(ArgumentError) { rpc.jobs('mail').push('t', headers: { 'attempt' => 1 }) }
  ensure
    client&.close
    server&.close
  end

  def test_a_push_with_no_id_is_a_protocol_error
    # Unreachable against this server, and checked because the alternative is returning an empty
    # string for a job that was pushed -- which an application logs as "the queue is broken" on a
    # call that succeeded.
    error = assert_raises(Volucer::ProtocolError) do
      converse(ok({ 'pushed' => true })) { |rpc| rpc.jobs('mail').push('t') }
    end

    assert_includes error.message, 'without naming the job'
  end

  def test_a_partial_batch_says_where_it_stopped_and_what_made_it
    # A batch is not atomic, so a caller whose third push failed has two jobs in the pipeline. Telling
    # it "the batch failed" would make its retry duplicate them.
    error = assert_raises(Volucer::ProtocolError) do
      converse(ok({ 'pushed' => 2, 'ids' => %w[job-1 job-2], 'failed_index' => 2, 'error' => 'the pipeline is full' })) do |rpc|
        rpc.jobs('mail').push_batch([{ task: 'a' }, { task: 'b' }, { task: 'c' }])
      end
    end

    assert_includes error.message, 'stopped at index 2'
    assert_includes error.message, 'the pipeline is full'
    assert_includes error.message, 'job-1, job-2', 'the caller needs the ids to retry only the tail'
  end

  def test_a_full_batch_answers_the_ids
    result, written = converse(ok({ 'pushed' => 2, 'ids' => %w[job-1 job-2], 'failed_index' => nil })) do |rpc|
      rpc.jobs('mail').push_batch([{ task: 'a' }, { task: 'b', payload: 'x' }])
    end

    assert_equal 'jobs.push_batch', written[0]['payload']['name']
    assert_equal 2, written[0]['payload']['params']['jobs'].length
    assert_equal b64('x'), written[0]['payload']['params']['jobs'][1]['payload']
    assert_equal %w[job-1 job-2], result
  end

  def test_an_empty_batch_is_not_sent
    # Nothing to push is not a call: a server asked to push zero jobs would have to invent an answer,
    # and a caller that passed an empty list gets an empty list back without a round trip.
    client, server = pair
    rpc = Volucer::Rpc.new(client, 't', '127.0.0.1:1', 1.0)

    assert_equal [], rpc.jobs('mail').push_batch([])
    assert_equal :wait_readable, server.read_nonblock(1, exception: false),
                 'an empty batch wrote a frame'
  ensure
    client&.close
    server&.close
  end

  def test_size_answers_both_numbers
    result, written = converse(ok({ 'size' => 3, 'in_flight' => 1 })) { |rpc| rpc.jobs('mail').size }

    assert_equal 'jobs.size', written[0]['payload']['name']
    assert_equal({ 'size' => 3, 'in_flight' => 1 }, result)
  end

  def test_pipelines_lists_the_deployment
    result, written = converse(ok({ 'pipelines' => [{ 'name' => 'mail', 'driver' => 'memory' }] })) do |rpc|
      rpc.jobs('mail').pipelines
    end

    assert_equal 'jobs.pipelines', written[0]['payload']['name']
    assert_equal [{ 'name' => 'mail', 'driver' => 'memory' }], result
  end

  # ------------------------------------------------------------- serializers

  def test_a_custom_serializer_replaces_json_both_ways
    # The server never sees a difference: it stores whatever bytes `encode` returns.
    _result, written = converse(ok({ 'stored' => true })) do |rpc|
      rpc.kv('cache').serializer(->(value) { "raw:#{value}" }, ->(raw) { raw.delete_prefix('raw:') })
             .set('k', 'v')
    end

    assert_equal b64('raw:v'), written[0]['payload']['params']['value']

    result, = converse(ok({ 'value' => b64('raw:v') })) do |rpc|
      rpc.kv('cache').serializer(->(value) { "raw:#{value}" }, ->(raw) { raw.delete_prefix('raw:') })
             .get('k')
    end
    assert_equal 'v', result
  end
end
