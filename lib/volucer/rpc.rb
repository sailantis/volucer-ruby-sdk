# frozen_string_literal: true

# The call plane for an application that has no worker channel.
#
# A `Volucer::Worker` reaches `kv`, `lock` and `jobs` on the stream that carries its requests. A
# FastCGI application has no such stream -- it exists for the duration of one request and can only
# answer the frame it was handed -- so volucer serves the same namespaces on a socket of its own,
# `[rpc]`, which this class dials:
#
#   require 'volucer'
#
#   rpc = Volucer::Rpc.connect(env: env)     # `env` is the Rack environment
#
#   rpc.kv('cache').set('greeting', 'hello')
#   holder = rpc.lock.acquire('pdf:create')
#   rpc.jobs('mail').push('SendEmail', payload: '{"to":"a@b"}')
#
# Nothing here is configured, and that is the point. The address and the backend's identity are
# **in the request's environment**, put there by volucer -- `VOLUCER_RPC` and `VOLUCER_RPC_TOKEN`
# as CGI parameters, or `X-Volucer-Rpc` and `X-Volucer-Rpc-Token` as headers for an `upstream`
# origin, which Rack presents as `HTTP_X_VOLUCER_RPC`. One deployment states the address once, in
# one file, and both sides read it; an application never has a second copy to keep in step.
#
# A backend whose `services` list is empty is handed neither, which is why {.connect} does not raise
# when they are missing: the application gets a plane that refuses every call with a message naming
# the missing variable. A request that never touches a store therefore still works, and the one that
# does gets an error pointing at the deployment rather than at this class.
#
# ## The wire is the worker protocol's, minus the handshake
#
# The same {Header}, the same `CALL`/`RETURN`, the same CRC-32C. What a worker establishes once in
# `HELLO`/`HELLO_ACK` -- version, checksum policy, process identity -- would be paid for on every
# request by a CGI process, so instead **each call carries the token**. That also means a grant
# withdrawn by a reload takes effect on the next call rather than at the next reconnect.
#
# One consequence worth knowing: there is no `HELLO`, so there is no offer to skip the checksum, so
# every frame this class writes is checksummed.
#
# ## The deadline is here, unlike a worker's channel
#
# `Worker#call` blocks with no timeout of its own, because the protocol guarantees an answer. This
# class reads a **socket**, so a deadline is both possible and necessary: a call whose answer was
# lost, or a server that stopped reading, would otherwise hang until the request timed out -- and
# that failure names the wrong thing.
#
# The default matches `[rpc].call_timeout_seconds` in the default deployment, and **a call that
# legitimately takes longer needs a longer deadline**: {Lock#acquire} may wait, so a caller asking
# for a long wait must say so -- `Rpc.connect(timeout: 30)`.
#
# ## A failure raises, and says whether it may have happened
#
# These methods return values and raise {ProtocolError} for a failure, which is what a Ruby
# application expects and what `Volucer::Worker#call` already does. The distinction that has to
# survive is {ProtocolError#unknown?}: true when the call was **written** and the answer never
# arrived, so retrying may duplicate a side effect; false when nothing was sent, or when the server
# answered a refusal.

require 'socket'
# `IO#wait_readable` is what gives a read a deadline, and it is not in core on every supported Ruby
# -- `io/wait` has to be required explicitly on 3.1, which CI tests. Without it the deadline check
# below is false for a real socket, and the client would silently fall back to a blocking read with
# no timeout at all: the precise failure the deadline exists to prevent.
require 'io/wait'

require_relative 'protocol'
# For `Volucer.unix_path`, so the `unix:` prefix is spelled once in this gem rather than twice. The
# dependency is one direction -- `worker` knows nothing about this file -- so the require is safe in
# either order.
require_relative 'worker'

module Volucer
  # The `[rpc]` call plane, as a client.
  #
  # Built by {.connect} from the request's environment, or constructed directly with a connected
  # stream -- which is what the tests do, because every interesting case here is about what the
  # client does with a particular answer.
  class Rpc
    # The CGI parameter (or upstream header) carrying the call plane's address.
    ENDPOINT_PARAM = 'VOLUCER_RPC'

    # The CGI parameter (or upstream header) carrying this backend's token.
    TOKEN_PARAM = 'VOLUCER_RPC_TOKEN'

    # The header form an `upstream` origin is given, as Rack spells it in its environment.
    ENDPOINT_HEADER = 'HTTP_X_VOLUCER_RPC'

    # The header form of the token.
    TOKEN_HEADER = 'HTTP_X_VOLUCER_RPC_TOKEN'

    # The default deadline for one call, in seconds.
    #
    # The same number `[rpc].call_timeout_seconds` defaults to, so a deployment that configured
    # nothing behaves the same on both sides.
    DEFAULT_TIMEOUT = 10.0

    # How long to wait for a connection, in seconds.
    #
    # A **cap**, not a second budget: {#call} gives the whole round trip one deadline, and this bounds
    # the dial's share of it. Without a cap, a dial to an address that is up but unresponsive would
    # consume the entire call deadline and leave the read none -- so the failure would name the read
    # path rather than the connection. The default deployment's `[rpc].call_timeout_seconds` is 10, so
    # half of it is a dial nobody can honestly call slow, and a legitimate remote listener answers in
    # milliseconds.
    DIAL_SHARE = 0.5

    # The first call id. Ids descend from the top of the range, matching the SDKs and the server.
    FIRST_CALL_ID = 0xFFFFFFFF

    # The last failure's message, if some call did not return a result.
    attr_reader :last_call_error

    # The address this plane dials, or nil when the deployment granted this backend nothing.
    #
    # For a diagnostic -- "which call plane is this application talking to?" is the first question
    # when a store works in one backend and not another. The address is in the request's environment
    # either way, so this publishes nothing new.
    attr_reader :address

    # Build a plane from the request's environment, or explicitly.
    #
    # Reads `VOLUCER_RPC`/`VOLUCER_RPC_TOKEN`, falling back to the `X-Volucer-Rpc*` header form an
    # `upstream` backend is handed. An `address:` or `token:` passed here wins, which is what a test
    # or a script needs.
    #
    # **`env:` is the Rack request's environment, and a Rack application should pass it.** volucer
    # puts the address in the *request*, which is a different thing from the process's own
    # environment: under FastCGI the values arrive as CGI parameters, and an `upstream` origin gets
    # them as headers. Reading `ENV` would therefore work for a script a deployment started by hand
    # and fail for every real request, with a message blaming the deployment. A CLI script or a queue
    # consumer omits it and gets `ENV`, which is correct for both -- and `ENV` answers the same `[]`
    # a Rack environment does, which is why this needs no type test.
    #
    # Returns a plane even when the deployment granted nothing: the *call* is what fails, with the
    # missing variable named. A request that never touches a store must still work, and the one that
    # does deserves a message about the deployment rather than an exception from a constructor it
    # did not know it was calling.
    #
    # **Nothing is dialled here.** The connection is made by the first call, so a request that never
    # touches a store pays for neither a socket nor a failed connect -- and a deployment whose call
    # plane is briefly unavailable does not fail a request that would not have used it.
    def self.connect(address: nil, token: nil, timeout: DEFAULT_TIMEOUT, env: nil)
      source = env.nil? ? ENV : env
      resolved = address.nil? ? first_value(source, ENDPOINT_PARAM, ENDPOINT_HEADER) : address
      resolved_token = token.nil? ? (first_value(source, TOKEN_PARAM, TOKEN_HEADER) || '') : token

      new(
        nil, resolved_token, resolved, timeout,
        unavailable: resolved.nil? ? unavailable_message(env) : nil
      )
    end

    # Why a plane with no address exists, in the words that name the variable.
    #
    # The message distinguishes "the process's environment" from "the request's", because those are
    # different mistakes: a CLI script reading `ENV` versus a backend whose `services` list is empty.
    def self.unavailable_message(env)
      where = env.nil? ? 'its environment' : 'the Rack request environment'
      "this application was handed no call plane: #{ENDPOINT_PARAM} is not in #{where}, which " \
        "means the backend's `services` list is empty or the deployment has no [rpc] listener"
    end

    # The first of `names` that holds a non-blank value, or nil.
    #
    # Blank rather than merely absent, because `VOLUCER_RPC=` is what a shell produces while somebody
    # is debugging, and an empty value is not an address to dial. The same rule
    # {Volucer.socket_address} applies to `VOLUCER_WORKER_SOCKET`.
    def self.first_value(source, *names)
      names.each do |name|
        value = source[name]
        next unless value.is_a?(String)

        value = value.strip
        return value unless value.empty?
      end
      nil
    end

    # Split an address into a unix path or a `[host, port]` pair.
    #
    # One method so the two forms are decided in one place, and so a malformed address is an error
    # at connect rather than an `Integer()` failure a frame later.
    def self.address_of(address)
      path = Volucer.unix_path(address)
      return [path, nil] unless path.nil?

      host, _, port = address.rpartition(':')
      unless port.match?(/\A\d+\z/)
        raise ArgumentError, "#{address.inspect} is neither a unix: path nor a host:port address"
      end

      [nil, [host.empty? ? '127.0.0.1' : host, Integer(port, 10)]]
    end

    # @param stream [IO, nil] the connection to speak over, or nil before the first call
    # @param token [String] the backend's identity, sent with every call
    # @param address [String, nil] where to dial, when a stream is not given
    # @param timeout [Float] how long one call may take, in seconds
    # @param unavailable [String, nil] why there is no address, for a plane the deployment granted
    #   nothing. Passed to the constructor rather than set afterwards so there is no moment in which a
    #   plane exists with neither an address nor an explanation.
    def initialize(stream, token = '', address = nil, timeout = DEFAULT_TIMEOUT, unavailable: nil)
      if timeout <= 0
        raise ArgumentError,
              'a call deadline has to be positive: zero would abandon every call the moment it was sent'
      end

      @stream = stream
      @token = token || ''
      @address = address
      @timeout = timeout
      # Why there is no stream **at construction time**, kept apart from `last_call_error` because
      # that one describes the last *call*. A plane built without an address would otherwise report
      # "the call plane could not be reached" -- or nothing at all -- instead of naming the variable
      # the deployment did not set.
      @unavailable = unavailable
      @last_call_error = unavailable
      @last_call_was_unknown = false
      @next_call_id = FIRST_CALL_ID
      @deadline = nil
    end

    # Whether the last failed call may have taken effect.
    #
    # True for a connection that broke or timed out **after the call was written** -- the server may
    # have served it and lost the answer. False for a call that was never sent, which is the whole
    # reason {.connect} records a missing address instead of raising: "the deployment granted this
    # backend nothing" is a definite non-event.
    def last_call_was_unknown?
      @last_call_was_unknown
    end

    # Whether there is a live connection to reuse.
    def connected?
      !@stream.nil?
    end

    # Drop the socket. The next call reconnects.
    #
    # Not needed before the process exits -- a CGI child's descriptors go with it -- but a caller that
    # keeps an `Rpc` in a constant, or a long-running CLI script, may want the descriptor back.
    def close
      stream = @stream
      @stream = nil
      return nil if stream.nil?

      stream.close if stream.respond_to?(:close)
      nil
    rescue IOError, SystemCallError
      nil
    end

    # ------------------------------------------------------------------ calling

    # One round trip. Returns the result, or raises {ProtocolError}.
    #
    # A `name` is `"<namespace>.<verb>"`, with the one exception the protocol documents: a **`kv`
    # call is three-part**, `"kv.<storage>.<verb>"`, because the deployment chooses the storage
    # names. {#kv}, {#lock} and {#jobs} build those names for you and are what an application should
    # normally use; this is the escape hatch for a namespace added to the server later.
    #
    # @raise [ProtocolError] if the call was refused, the connection failed, or the deadline expired.
    #   {ProtocolError#unknown?} says whether it may nevertheless have taken effect.
    def call(name, params = {})
      @last_call_error = nil
      @last_call_was_unknown = false
      # **One deadline for the whole call, dial included.** Two additive budgets -- a connect timeout
      # plus a read timeout -- make the worst case their sum, which is not what "one call may take
      # `call_timeout_seconds`" means to anyone reading the deployment's configuration.
      @deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @timeout

      # Recorded first, then raised: `ensure_stream` writes the message into `last_call_error`, and a
      # caller that rescues the exception may still ask this object why.
      raise failed(@last_call_error, unknown: false) unless ensure_stream

      call_id = take_call_id
      body = { 'name' => name, 'params' => params }
      # Omitted rather than sent empty when there is no token. The server tells the two apart on
      # purpose: "carried no token" is a deployment that granted this backend nothing, and "carried a
      # token no backend was issued" is an application reading the wrong variable.
      body['token'] = @token unless @token.empty?

      begin
        write_frame(Frame.new(MessageType::CALL, call_id, Volucer.json_bytes(body)))
      rescue IOError, SystemCallError => e
        # The bytes may or may not have reached the server: a write that failed part-way is exactly
        # the undecided case this flag exists for.
        close
        raise failed("call #{name.inspect} could not be written: #{e.message}", unknown: true)
      end

      await_return(call_id, name)
    end

    # ------------------------------------------------------------------ handles

    # A handle to one of the server's key-value storages.
    #
    # The storage must be one this backend was granted in `services`; a call naming another is
    # refused by the server with the storages that *are* reachable.
    def kv(storage = 'default')
      Kv.new(self, storage)
    end

    # A handle to the server's resource locks.
    #
    # Read {Lock}'s documentation before relying on one: a lease is not a fencing token, and that is
    # a property of leases rather than of this transport.
    def lock
      Lock.new(self)
    end

    # A handle to one of the server's queues, from the producer's side.
    #
    # @param pipeline [String] the name from the `[jobs.pipelines.<name>]` table.
    def jobs(pipeline)
      Jobs.new(self, pipeline)
    end

    # ----------------------------------------------------------------- transport

    private

    # Record what went wrong and build the exception to raise.
    #
    # One place, so a new failure path cannot set one and forget the other -- and `unknown` is a
    # required keyword, because choosing it is the decision that matters.
    def failed(message, unknown:)
      @last_call_was_unknown = unknown
      @last_call_error = message
      ProtocolError.new(message, fatal: false, unknown: unknown)
    end

    # The next call id, counting down and wrapping.
    #
    # Descending from the top of the range, like every other SDK: request ids and call ids are
    # deliberately from opposite ends, so a call mistaken for a request is a visible correlation
    # failure rather than a plausible answer.
    def take_call_id
      call_id = @next_call_id
      @next_call_id = call_id <= 1 ? FIRST_CALL_ID : call_id - 1
      call_id
    end

    # The stream, dialling if there is not one yet.
    #
    # One path for both cases: a plane that has never connected, and a plane whose connection a
    # previous call dropped. What must not be retried is a deployment that handed over no address at
    # all -- that is `@unavailable`, decided once, and re-dialling nothing would only replace its
    # message with a vaguer one.
    #
    # A failed *dial*, by contrast, is retried on the next call: it is the server being briefly
    # absent, and a plane that gave up for the life of the process would turn a two-second gap into a
    # permanently broken application.
    def ensure_stream
      return true unless @stream.nil?

      unless @unavailable.nil?
        @last_call_error = @unavailable
        return false
      end

      if @address.nil?
        @last_call_error = 'this call plane has no stream and no address to dial'
        return false
      end

      stream = dial(@address)
      if stream.nil?
        @last_call_error = "the call plane at #{@address} could not be reached"
        return false
      end

      @stream = stream
      true
    end

    # Connect, or nil -- a connection failure is a call's failure, not a constructor's.
    #
    # Bounded by what is left of the call's deadline, so a dial cannot consume the time the read
    # needs. {DIAL_SHARE} caps its share at half.
    def dial(address)
      path, tcp = Rpc.address_of(address)
      budget = dial_budget
      return nil if budget <= 0

      if path.nil?
        # `Socket.tcp` rather than `TCPSocket.new`: only the former takes a timeout.
        Socket.tcp(tcp[0], tcp[1], connect_timeout: budget)
      else
        UNIXSocket.new(path)
      end
    rescue IOError, SystemCallError, SocketError, ArgumentError
      nil
    end

    # How long the dial may take: half the plane's timeout, or what is left of this call if less.
    def dial_budget
      share = @timeout * DIAL_SHARE
      return share if @deadline.nil?

      [share, @deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)].min
    end

    # Write one frame, checksummed.
    #
    # No handshake happened, so nothing negotiated the checksum away and it is never optional here.
    def write_frame(frame)
      @stream.write(frame.encode(checksum: true))
      # A no-op for a socket, and correct for a `StringIO` in a test.
      @stream.flush if @stream.respond_to?(:flush)
      nil
    end

    # Read until a RETURN for `call_id` arrives.
    #
    # Every exit that leaves the call undecided -- the deadline, a broken connection, an answer that
    # belongs to another exchange -- closes the socket, because the stream can no longer be trusted
    # to be where this client thinks it is. A refusal does not: the server answered, so the stream is
    # aligned and the next call reuses it.
    def await_return(call_id, name)
      # Re-armed per call rather than per plane, so a plane reused across two calls gets two full
      # deadlines rather than one that expired during the first. `call` has already set it before the
      # dial, which is what makes the dial and the read share one budget; this only extends it when
      # `await_return` is reached without going through `call`.
      @deadline ||= Process.clock_gettime(Process::CLOCK_MONOTONIC) + @timeout

      loop do
        header, payload = read_frame
        if header.nil?
          # The server closed the connection after the call was written, so whether it was served is
          # not knowable from here.
          close
          raise failed(
            "call #{name.inspect} was written but the server closed the connection before answering",
            unknown: true
          )
        end

        unless header.message_type == MessageType::RETURN
          # Anything else is not part of this exchange. A worker absorbs telemetry here; this
          # transport never sees unsolicited frames, so a correlated frame is a protocol error rather
          # than something to skip.
          close
          raise failed(
            "while waiting for a RETURN the server sent a #{header.type_name}",
            unknown: true
          )
        end

        unless header.request_id == call_id
          close
          raise failed(
            "a RETURN for call #{header.request_id} arrived while waiting for call #{call_id}",
            unknown: true
          )
        end

        answer = begin
          Volucer.decode_json(payload, 'a RETURN')
        rescue ProtocolError => e
          # An answer this client cannot read is an answer it cannot act on, and the call may well
          # have been served: undecided, not refused.
          close
          raise failed("call #{name.inspect} was answered with a payload that is not a RETURN: #{e.message}",
                       unknown: true)
        end

        unless answer['ok']
          # A refusal carrying `unknown: true` says the effect may have happened; one without it says
          # nothing happened. The server decides which, and this passes it through rather than
          # collapsing the two.
          raise failed(
            answer.fetch('error', 'the call was refused'),
            unknown: answer['unknown'] == true
          )
        end

        return answer['result']
      end
    rescue Framing => e
      # A truncated frame, a checksum mismatch, an impossible payload length: the stream's position is
      # no longer knowable, so the connection is discarded rather than reused.
      close
      raise failed("call #{name.inspect} failed: #{e.message}", unknown: true)
    rescue Deadline
      close
      raise failed(
        "call #{name.inspect} did not answer within #{@timeout}s; it may have been served, so " \
        'retrying is not necessarily safe (a longer deadline: Rpc.connect(timeout: ...))',
        unknown: true
      )
    rescue IOError, SystemCallError => e
      close
      raise failed("call #{name.inspect} failed: #{e.message}", unknown: true)
    end

    # One frame, or nil at a clean end of stream between calls.
    #
    # The framing failures are wrapped in {Framing} so that {#await_return} can tell them -- which
    # must close the connection -- from the failures it has already converted into a {ProtocolError}
    # with its `unknown` flag chosen.
    def read_frame
      header = read_header
      return [nil, nil] if header.nil?

      [header, read_payload(header)]
    rescue ProtocolError => e
      raise Framing, e.message
    end

    # The next frame header, or nil at a clean end of stream.
    #
    # A stream that ends before the first byte is a clean close -- that is what a server does when its
    # idle timeout expires between calls. A stream that ends *part-way* through a header is an error,
    # because treating it as one would hide a truncated frame.
    def read_header
      raw = read_exactly(HEADER_LEN, allow_eof: true)
      return nil if raw.nil?

      Header.decode(raw)
    end

    # Read a frame's payload and verify its checksum.
    #
    # A mismatch means framing has already desynchronised, so it is fatal by construction.
    def read_payload(header)
      return ''.b if header.payload_len.zero?

      # Refused *before* reading, which is the point of the limit: a corrupt `payload_len` must not
      # make the reader allocate 4 GiB.
      if header.payload_len > MAX_PAYLOAD_LEN
        raise ProtocolError,
              "a frame declares #{header.payload_len} payload bytes, above the " \
              "#{MAX_PAYLOAD_LEN} limit; refusing it before allocating"
      end

      payload = read_exactly(header.payload_len)
      return payload if header.declares_no_checksum?

      computed = Crc32c.digest(payload)
      return payload if computed == header.checksum

      raise ProtocolError,
            'a RETURN arrived with a checksum that does not match ' \
            "(expected #{format('0x%08x', header.checksum)}, computed #{format('0x%08x', computed)})"
    end

    # Exactly `length` bytes, or nil at a clean end of stream when `allow_eof` and nothing was read.
    #
    # The deadline is applied per read rather than once, so a peer that dribbles a frame out cannot
    # hold a call open past the timeout by sending one byte at a time.
    def read_exactly(length, allow_eof: false)
      buffer = +''.b
      while buffer.bytesize < length
        chunk = read_some(length - buffer.bytesize, allow_eof && buffer.empty?)
        if chunk.nil?
          return nil if allow_eof && buffer.empty?

          raise ProtocolError,
                "the stream ended after #{buffer.bytesize} of #{length} bytes"
        end
        buffer << chunk
      end
      buffer
    end

    # Up to `length` bytes from the stream, or nil at end of stream.
    def read_some(length, allow_eof)
      return read_blocking(length, allow_eof) unless deadline_capable?

      loop do
        chunk = @stream.read_nonblock(length, exception: false)
        case chunk
        when :wait_readable
          remaining = @deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
          raise Deadline if remaining <= 0

          # nil from `wait_readable` is the timeout expiring; the loop recomputes and raises.
          @stream.wait_readable(remaining)
        when :wait_writable
          # Unreachable for a read, and handled rather than left to `NoMethodError` on a Symbol.
          raise ProtocolError, 'the stream asked to wait for writability during a read'
        when nil
          return nil
        else
          return chunk
        end
      end
    end

    # The fallback for a stream with no non-blocking read -- a `StringIO` in a test.
    #
    # No deadline, and that is honest: the deadline is a property of the transport. What must not
    # happen is a *socket* taking this path, which is what {#deadline_capable?} is for.
    def read_blocking(length, allow_eof)
      chunk = @stream.read(length)
      return nil if chunk.nil? || chunk.empty?

      chunk
    rescue EOFError
      return nil if allow_eof

      raise
    end

    # Whether the stream can be given a deadline.
    #
    # A socket can: `read_nonblock` plus `wait_readable` is a bounded read. A `StringIO` cannot, and
    # an earlier version of this client assumed every stream could -- so a deadline applied to
    # something that silently ignored it left the read blocking for ever. The test is on the
    # *capability*, not on the class, so a test double that implements both gets the real path.
    def deadline_capable?
      @stream.respond_to?(:read_nonblock) && @stream.respond_to?(:wait_readable)
    end

    # Internal: the deadline expired. Never escapes this file.
    class Deadline < StandardError; end

    # Internal: the stream's framing is no longer trustworthy. Never escapes this file.
    class Framing < StandardError; end
  end
end
