# frozen_string_literal: true

# The namespaces, reached through a `Volucer::Rpc` call plane.
#
# These are the client halves of `kv`, `lock` and `jobs`, and they are written against `Rpc`'s
# `call`/`last_call_error` rather than against a socket, so the same classes work over any transport
# a future SDK version adds.
#
# ## Bytes, and where they are base64
#
# The server stores opaque bytes and never parses an application's objects, which is what makes a
# serializer possible at all. The JSON codec cannot carry a byte above `0x7f`, so **every field the
# server treats as bytes is base64 on the wire** -- a KV key, a KV value, a job payload. A lock's
# `resource` is the exception: it is a *name*, read as text.
#
# That asymmetry is not cosmetic. `"greeting"` is itself well-formed base64, so a client that forgot
# to encode a key looks like a client storing a strange key rather than a client with a bug; `"a"` is
# refused. See `docs/worker-protocol.md`, "What a call's params carry".
#
# ## Keys and values are Strings or bytes
#
# A KV key is a `String` or a byte `String` (Ruby has only the one type; what matters is the
# encoding). A `Symbol` or an `Integer` is refused here rather than by the server, because a key that
# is a number is a caller's bug and a message from this file names the line.
#
# A KV value is JSON-encoded by default, so `set('k', { 'a' => 1 })` returns the Hash it was given.
# {Volucer::Kv#serializer} replaces that with any pair of callables.

require 'json'

require_relative 'protocol'

module Volucer
  # Base64 the way the protocol spells it: strict, with padding.
  #
  # `Array#pack('m0')` and `String#unpack1('m0')` are the standard library's strict forms, which is
  # why this does not need the `base64` gem (a bundled gem since Ruby 3.4, and therefore one more
  # thing a worker could fail to load). The strictness matters in one direction only: a malformed
  # encoding of a *server's* answer is a bug this client should not paper over.
  module Base64Codec
    # Encode bytes, with padding.
    def self.encode(bytes)
      [bytes.b].pack('m0')
    end

    # Decode base64, refusing anything that is not.
    #
    # `ArgumentError` rather than a protocol error: this is called on a caller's own input when a key
    # or a value is being built as often as on the server's answer, and the two callers want
    # different exception classes. {Volucer::Kv} turns it into a {ProtocolError} for the answer path.
    def self.decode(text)
      text.unpack1('m0')
    end
  end

  # One key-value storage, reached through a call plane.
  #
  # The shape is PSR-16's, which the server implements: a missing key is `nil` rather than an
  # exception, and a *failure* is an exception, because those are different things and a cache that
  # returned `nil` for a broken server would be a cache that silently stopped caching.
  class Kv
    # The storage name this handle reaches.
    attr_reader :storage

    def initialize(plane, storage = 'default')
      @plane = plane
      @storage = storage
      @encode = ->(value) { Volucer.json_bytes(value) }
      @decode = ->(raw) { Kv.decode_value(raw) }
    end

    # The value of a key, or `nil`.
    #
    # A *failure* raises, so a miss and a broken server look different.
    def get(key)
      answer = ask('get', 'key' => Kv.key_of(key))
      encoded = answer['value']
      return nil if encoded.nil?

      @decode.call(Kv.unbase64(encoded))
    end

    # Whether a key exists, without copying its value out of the store.
    def has?(key)
      answer = ask('has', 'key' => Kv.key_of(key))
      answer['present'] == true
    end

    # Write a value, optionally with a TTL in **seconds**.
    def set(key, value, ttl: nil)
      answer = ask('set', Kv.written(key, value, @encode).merge(Kv.ttl_param(ttl)))
      answer['stored'] == true
    end

    # Write **only if the key does not exist**, atomically on the storage side.
    #
    # `false` is a *successful* answer -- somebody else got there first -- and must not be read as a
    # failure. This is the one method here that can protect an invariant: a lock has no fencing token
    # and cannot stop a late writer, and a conditional write can, because the storage decides.
    def set_if_absent(key, value, ttl: nil)
      answer = ask('setIfAbsent', Kv.written(key, value, @encode).merge(Kv.ttl_param(ttl)))
      answer['stored'] == true
    end

    # Remove a key, answering whether it was there.
    def delete(key)
      answer = ask('delete', 'key' => Kv.key_of(key))
      answer['deleted'] == true
    end

    # The remaining TTL: a number of seconds, or `'missing'` / `'forever'`.
    #
    # A word rather than `-1`, because the three states are not a range and a caller checking
    # `seconds > 0` would read "never expires" as "expired".
    def ttl(key)
      ask('ttl', 'key' => Kv.key_of(key))['ttl']
    end

    # Empty the storage.
    def clear
      ask('clear', {})['cleared'] == true
    end

    # Several keys at once, as a Hash of the ones that were there.
    #
    # Missing keys are **absent from the Hash** rather than present with a `nil`, which is what makes
    # `{ 'a' => nil }` -- a key holding JSON `null` -- distinguishable from a key that is not stored.
    def get_multiple(keys)
      ordered = keys.to_a
      values = ask('getMultiple', 'keys' => ordered.map { |key| Kv.key_of(key) })['values'] || []
      found = {}
      ordered.each_with_index do |key, index|
        encoded = index < values.length ? values[index] : nil
        found[key] = @decode.call(Kv.unbase64(encoded)) unless encoded.nil?
      end
      found
    end

    # Several keys at once, keeping the *positions*: `nil` for each key that is missing.
    def get_many(keys)
      ordered = keys.to_a
      values = ask('getMultiple', 'keys' => ordered.map { |key| Kv.key_of(key) })['values'] || []
      ordered.each_index.map do |index|
        value = index < values.length ? values[index] : nil
        value.nil? ? nil : @decode.call(Kv.unbase64(value))
      end
    end

    # Store several values at once, all with the same TTL in **seconds**.
    def set_multiple(values, ttl: nil)
      pairs = values.to_h
      encoded = pairs.each_with_object({}) do |(key, value), out|
        out[Kv.key_of(key)] = Base64Codec.encode(@encode.call(value))
      end
      answer = ask('setMultiple', { 'values' => encoded }.merge(Kv.ttl_param(ttl)))
      answer['stored'].to_i == encoded.length
    end

    # Replace the JSON serializer with a pair of callables.
    #
    # The server never sees a difference: it stores whatever bytes `encode` returns.
    def serializer(encode, decode)
      @encode = encode
      @decode = decode
      self
    end

    # The default deserializer: JSON, falling back to bytes.
    #
    # A value written by `set` is always JSON. A value written by another application through the
    # same storage -- a PHP worker, say -- need not be, and returning the bytes is the honest answer
    # rather than an exception that would make one storage unusable by two languages.
    #
    # **The encoding is checked before parsing, rather than relying on `JSON.parse` to raise.** It
    # does not, consistently: json 2.x as shipped with Ruby 3.1 and 3.3 returns a String for a
    # binary argument it cannot decode, where the version local testing had raised `ParserError` --
    # so a test that passed on one machine failed in CI with a value that was neither the bytes nor
    # the JSON. Bytes that are not valid UTF-8 cannot be JSON text by definition, which is a fact
    # about the data rather than about the library.
    def self.decode_value(raw)
      text = raw.dup.force_encoding(Encoding::UTF_8)
      return raw unless text.valid_encoding?

      JSON.parse(text)
    rescue JSON::ParserError
      raw
    end

    # A KV key as the wire spells it: base64 of its bytes.
    def self.key_of(key)
      unless key.is_a?(String)
        raise TypeError, "a kv key must be a String, not #{key.class}"
      end
      if key.empty?
        raise ArgumentError,
              'a kv key must not be empty; the server refuses one rather than storing it'
      end

      Base64Codec.encode(key)
    end

    # Base64 of a value's JSON encoding, or of the bytes given.
    #
    # A value that is bytes is sent as given rather than re-encoded: a caller who compressed or
    # encrypted it should not have it wrapped in a JSON string, which would double its size and change
    # what a consumer reads.
    def self.written(key, value, encode)
      raw = bytes?(value) ? value : encode.call(value)
      { 'key' => key_of(key), 'value' => Base64Codec.encode(raw) }
    end

    # Whether a value is bytes rather than something to serialise.
    #
    # **Two tests, and the second is the one that matters in practice.** A `File.binread` is tagged
    # `BINARY`; a `File.read` of the same file is tagged UTF-8 and is not valid UTF-8, which Ruby
    # permits and the JSON encoder refuses with `JSON::GeneratorError` from inside the library. A
    # string that cannot be JSON text *is* bytes, so treating it as such is both the only workable
    # answer and the honest one -- and it means the failure a caller sees is never a `GeneratorError`
    # naming no argument of theirs.
    def self.bytes?(value)
      value.is_a?(String) && (value.encoding == Encoding::BINARY || !value.valid_encoding?)
    end

    # The `ttl` parameter, or nothing.
    #
    # An empty Hash rather than a `nil` value: the server reads `ttl >= 0` as a TTL and absent as
    # "no expiry", and sending `null` for the absent case would be a third thing to decode.
    def self.ttl_param(ttl)
      ttl.nil? ? {} : { 'ttl' => Integer(ttl) }
    end

    # Decode a server-supplied value, turning a bad encoding into a protocol error.
    def self.unbase64(text)
      Base64Codec.decode(text)
    rescue ArgumentError => e
      raise ProtocolError, "the server sent a value that is not valid base64: #{e.message}"
    end

    private

    # One call, with a failure raised naming the storage and the reason.
    def ask(verb, params)
      name = "kv.#{@storage}.#{verb}"
      answer = @plane.call(name, params)
      answer.is_a?(Hash) ? answer : {}
    rescue ProtocolError => e
      # Re-raised with the storage in the message, because "kv.get failed" does not say which store
      # -- and the server's own refusal already names the ones this backend *may* reach, which is the
      # sentence an operator needs.
      raise ProtocolError.new("#{name} failed: #{e.message}", fatal: e.fatal, unknown: e.unknown)
    end
  end

  # The resource locks, reached through a call plane.
  #
  # A lock is for **collapsing work** -- two requests rendering the same PDF -- and not for
  # protecting an invariant. A lease expires, and an expired lease is not a fencing token: a process
  # that was paused past its TTL and then resumed will keep writing. That rule is the same one the
  # worker channel's lock documents, because it is a property of leases rather than of the transport.
  class Lock
    def initialize(plane)
      @plane = plane
    end

    # Take the lock, returning the id that holds it, or nil for contention.
    #
    # `ttl` and `wait` are **microseconds**. Zero `ttl` means "held until released" and zero `wait`
    # means one attempt. Contention is a successful call with `acquired: false`, not an error --
    # nothing failed, the caller just knows it is not in.
    #
    # `mode: :shared` takes a shared lock instead, which other readers may hold too while an
    # exclusive holder excludes it. It defaults to exclusive, the reading a caller who did not think
    # about it wants: a shared lock is an optimisation for a read path, and defaulting to it would
    # silently stop two writers from excluding each other.
    #
    # A `wait` longer than the plane's timeout needs a longer timeout: the deadline is the client's,
    # and a call abandoned mid-wait leaves the lock's state unknown -- which is why that case raises
    # with {ProtocolError#unknown?} true and the remedy is `exists?`.
    def acquire(resource, ttl: 0, wait: 0, mode: :exclusive, id: nil)
      params = {
        'resource' => resource, 'ttl' => Integer(ttl), 'wait' => Integer(wait),
        'mode' => Lock.mode_of(mode)
      }
      params['id'] = id unless id.nil?

      answer = ask('acquire', params)
      return nil unless answer['acquired'] == true

      answer['id']
    end

    # One attempt with no waiting at all.
    #
    # Equivalent to `acquire(resource, wait: 0)`, and its own method because the server has one:
    # `wait = 0` on a *contended* lock still costs a lookup, and saying "do not wait" is saying "one
    # attempt, not a deadline that happens to be now".
    def try_acquire(resource, ttl: 0, mode: :exclusive, id: nil)
      params = { 'resource' => resource, 'ttl' => Integer(ttl), 'mode' => Lock.mode_of(mode) }
      params['id'] = id unless id.nil?

      answer = ask('try_acquire', params)
      return nil unless answer['acquired'] == true

      answer['id']
    end

    # Release a lock this caller holds, answering whether there was one.
    #
    # `false` means the id did not hold the resource -- the lease expired, or something released it --
    # and that is an **answer rather than an error**, because "my lock is already gone" is not
    # something an `ensure` block wants to raise for.
    def release(resource, id)
      ask('release', 'resource' => resource, 'id' => id)['released'] == true
    end

    # Release **every** lock on the resource, whoever holds them.
    #
    # The escape hatch for a resource wedged by a worker that died. It cannot be scoped by id and is
    # not, which is why the server puts it behind the same grant that allows locking at all.
    def force_release(resource)
      ask('release_all', 'resource' => resource)['released'] == true
    end

    # Whether anything holds the resource.
    #
    # The method to call when an acquisition raised with an unknown outcome: it is the question that
    # resolves it, and the server derives it from the same code that reports holders, so the two
    # cannot disagree.
    def exists?(resource)
      ask('exists', 'resource' => resource)['exists'] == true
    end

    # Every holder of the resource, with what is left of each lease.
    #
    # A list of `{ 'id' => String, 'mode' => 'exclusive'|'shared', 'remaining_ms' => Integer|nil }`,
    # where `nil` is a lease that does not expire. An expired holder is not reported, whether or not
    # the backend has got round to removing it.
    def holders(resource)
      answer = ask('holders', 'resource' => resource)['holders']
      answer.is_a?(Array) ? answer : []
    end

    # Set what is left of this id's lease, from now, in **microseconds**.
    #
    # **Replaces the deadline rather than extending it**: renewing for ten microseconds ten
    # microseconds into a lease of twenty leaves ten, not twenty. A caller that reads it as "another
    # N" can be released early.
    #
    # `false` means the id held nothing, which is the answer to act on -- the section being protected
    # is no longer protected -- and not an error, because the call itself succeeded.
    def update_ttl(resource, id, ttl)
      ask('update_ttl', 'resource' => resource, 'id' => id, 'ttl' => Integer(ttl))['updated'] == true
    end

    # The wire's spelling of a mode.
    #
    # Both spellings the server accepts are accepted here, and a Symbol is what a Ruby caller writes,
    # so `:read` works as well as `:shared`. Anything else is refused here rather than by the server,
    # because a mode that is a typo is a caller's bug and this message names the line.
    def self.mode_of(mode)
      case mode.to_s
      when 'exclusive', 'write' then 'exclusive'
      when 'shared', 'read' then 'shared'
      else
        raise ArgumentError, "a lock mode is :exclusive or :shared, not #{mode.inspect}"
      end
    end

    private

    def ask(verb, params)
      name = "lock.#{verb}"
      answer = @plane.call(name, params)
      answer.is_a?(Hash) ? answer : {}
    rescue ProtocolError => e
      raise ProtocolError.new("#{name} failed: #{e.message}", fatal: e.fatal, unknown: e.unknown)
    end
  end

  # One queue pipeline, from the producer's side.
  #
  # The payload is the application's own bytes and the server never parses it -- the consuming side
  # is a worker with a `JOB` handler, and what it finds in the body is whatever this side put there.
  class Jobs
    # The pipeline this handle pushes to.
    attr_reader :pipeline

    def initialize(plane, pipeline)
      @plane = plane
      @pipeline = pipeline
    end

    # Enqueue one job, answering the id the server assigned.
    #
    # `task` is the name the consuming side dispatches on and is **required**: the server refuses a
    # push without one rather than guessing from the pipeline, because a queue whose task name was
    # implicit would put every job on a pipeline into one handler.
    #
    # `payload` is a String of the application's own bytes, sent as given. A Hash or an Array is
    # JSON-encoded, and `nil` is the empty payload -- which is what a job whose whole instruction is
    # its task name sends.
    #
    # `headers` are string metadata such as `traceparent`. A non-string value is refused here rather
    # than by the server: a header that was meant to be in the payload is a caller's bug.
    def push(task, payload: '', headers: {})
      if task.nil? || task.to_s.empty?
        raise ArgumentError, 'a job needs a task name, or its consumer cannot tell what it is'
      end

      params = {
        'pipeline' => @pipeline,
        'task' => task.to_s,
        'payload' => Base64Codec.encode(Jobs.payload_of(payload)),
        # An empty Hash is sent as `{}` rather than omitted: the server reads an absent map and an
        # empty one the same way, and a client that varies its params by emptiness is a client whose
        # wire format has two shapes for one call.
        'headers' => Jobs.string_headers(headers)
      }

      id = ask('push', params)['id']
      if !id.is_a?(String) || id.empty?
        # Unreachable against this server, and checked because the alternative is returning an empty
        # string for a job that was pushed -- which an application would log as "the queue is broken"
        # on a call that succeeded.
        raise ProtocolError, 'the server accepted a push without naming the job'
      end

      id
    end

    # Enqueue several jobs at once, answering the ids that were accepted.
    #
    # **A batch is not atomic, and this throws honestly about it**: a batch whose third push fails has
    # already pushed two, and reporting that as a total failure would make a caller's retry duplicate
    # them. So a partial batch raises with the ids that made it in the message, and the caller decides
    # whether to retry the tail.
    #
    # @param jobs [Array<Hash>] each with `:task` and optionally `:payload` and `:headers`.
    # @return [Array<String>] the ids, in the order the jobs were given.
    def push_batch(jobs)
      entries = jobs.each_with_index.map do |job, index|
        unless job.is_a?(Hash) && !job[:task].to_s.empty?
          raise ArgumentError, "the job at index #{index} needs a task name"
        end

        {
          'pipeline' => @pipeline,
          'task' => job[:task].to_s,
          'payload' => Base64Codec.encode(Jobs.payload_of(job[:payload])),
          'headers' => Jobs.string_headers(job[:headers] || {})
        }
      end

      return [] if entries.empty?

      answer = ask('push_batch', 'jobs' => entries)
      ids = answer['ids'].is_a?(Array) ? answer['ids'].select { |id| id.is_a?(String) } : []

      unless answer['failed_index'].nil?
        raise ProtocolError,
              "the batch stopped at index #{answer['failed_index']}: #{answer['error'] || 'no reason given'}; " \
              "the #{ids.length} jobs before it were pushed (#{ids.join(', ')}) -- retry from there"
      end

      ids
    end

    # How many jobs are ready, and how many are held by a consumer.
    #
    # Both numbers, because the pair is what an operator reads: a pipeline whose ready count grows
    # while its in-flight count stays flat is a consumer that is stuck, and neither number says that
    # alone.
    #
    # @return [Hash] `{ 'size' => Integer, 'in_flight' => Integer }`
    def size
      answer = ask('size', 'pipeline' => @pipeline)
      { 'size' => answer['size'].to_i, 'in_flight' => answer['in_flight'].to_i }
    end

    # Every pipeline this deployment has, with its driver.
    #
    # For a diagnostic, and for the refusal a typo in a pipeline name produces -- which is where the
    # list is most useful. It is *every* pipeline, not the ones this backend may push to, because a
    # queue is not scoped per backend the way a KV storage is -- which is also why the answer does not
    # depend on the handle it was asked through, and why this matches the PHP SDK's `Jobs::pipelines`
    # rather than taking no receiver at all.
    def pipelines
      answer = ask('pipelines', {})
      list = answer['pipelines']
      list.is_a?(Array) ? list : []
    end

    # A payload's bytes: as given when it is a String, JSON otherwise.
    #
    # The same rule {Kv.bytes?} follows, and the reason it is a little broader here: a caller writes
    # `payload: '{"to":"a@b"}'` far more often than it hands over binary, so a *valid* UTF-8 String is
    # the bytes verbatim while a String that cannot be JSON text is treated as bytes too. Only a
    # non-String -- a Hash, an Array -- is JSON-encoded.
    def self.payload_of(payload)
      return ''.b if payload.nil?
      return payload.b if payload.is_a?(String)

      Volucer.json_bytes(payload)
    end

    # The header map, refusing a non-string value.
    def self.string_headers(headers)
      headers.each_with_object({}) do |(name, value), out|
        unless name.is_a?(String) && value.is_a?(String)
          raise ArgumentError,
                "the header #{name.inspect} must be a pair of Strings: headers are metadata, and a " \
                'structured value belongs in the payload'
        end

        out[name] = value
      end
    end

    private

    def ask(verb, params)
      name = "jobs.#{verb}"
      answer = @plane.call(name, params)
      answer.is_a?(Hash) ? answer : {}
    rescue ProtocolError => e
      raise ProtocolError.new("#{name} failed: #{e.message}", fatal: e.fatal, unknown: e.unknown)
    end
  end
end
