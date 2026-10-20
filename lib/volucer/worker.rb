# frozen_string_literal: true

# The worker loop: handshake, serve, and the exchanges that run beside it.
#
# The shape is the PHP and Python SDKs', because the three implement one specification: a
# handshake, a read loop that dispatches on the frame type, and a response that is a head,
# zero or more body chunks, then a terminal frame.
#
# Two things here are Ruby's own answer rather than a transliteration:
#
# **Output.** The protocol uses the child's stdout as its channel. PHP rebinds `echo` with
# an output buffer and the Python SDK rebinds `sys.stdout`; Ruby's answer is that `$stdout`
# is reassigned to `$stderr` when this file is loaded, and the real protocol stream is kept
# in a private reference. `puts` therefore still reaches a human (through stderr, which the
# supervisor drains), but it cannot land inside a frame.
#
# **RSS.** The protocol's `peak_memory_bytes` drives recycling on `memory_limit_mb`, and it
# is meant *per request*. Ruby has no exact per-request answer either: `GC.stat` reports
# heap slots rather than bytes of resident memory, so this SDK reports the process's RSS as
# the platform gives it. That is cumulative and cannot fall, so a peak that once exceeded
# the limit recycles the worker after every request — which the README states, because it
# changes how a deployment should configure `memory_limit_mb`.

require 'socket'

require_relative 'protocol'
require_relative 'message'

module Volucer
  # The environment variable naming a socket to connect to instead of using stdio.
  #
  # Set by the server on a platform where a pipe is not the transport — Windows named
  # pipes — so a worker that supports it is reachable there. Absent, empty or whitespace
  # means stdio, because `VOLUCER_WORKER_SOCKET=` is what a shell produces from a stray
  # space and connecting to it would be a confusing failure rather than the obvious one.
  SOCKET_ENV = 'VOLUCER_WORKER_SOCKET'

  # The prefix volucer's configuration uses for a unix socket address.
  #
  # The same string `Volucer::Supervisor::UNIX_PREFIX` holds, and deliberately a separate
  # constant: one side dials and the other listens, and either moving without the other is
  # a bug worth being able to see.
  UNIX_PREFIX = 'unix:'

  # The environment variable naming the relay an `attach` backend dials.
  #
  # The counterpart of `SOCKET_ENV`, and for the same purpose: one address both halves of a
  # deployment must agree on, written once. The difference is direction. `SOCKET_ENV` is set *by*
  # the server, on a child it spawned, so a supervisor cannot use it -- there is no spawned child,
  # and the value there would be whatever an enclosing environment happened to leave. This one is
  # set by the *operator*, in the unit file or the container environment, and both the server's
  # `relay` and `Volucer::Supervisor.serve` read it.
  #
  # Absent, empty or whitespace means unset, for the same reason `SOCKET_ENV` does.
  RELAY_ENV = 'VOLUCER_RELAY_SOCKET'

  # The package this SDK is, reported in `runtime`.
  PACKAGE = 'volucer-worker-ruby'

  # The socket the server asked to be reached at, or nil for stdio.
  def self.socket_address
    address = ENV.fetch(SOCKET_ENV, nil)
    return nil if address.nil?

    address = address.strip
    address.empty? ? nil : address
  end

  # The filesystem path of a `unix:` address, or nil for a TCP one.
  #
  # The same convention volucer's configuration uses for `external.socket`, `attach.relay`
  # and a worker's `socket` key, so this reads the rule rather than inventing a fourth
  # spelling. A loopback TCP address is `host:port` and has no prefix, which is why the
  # absence of one is the whole test.
  def self.unix_path(address)
    return nil unless address.start_with?(UNIX_PREFIX)

    address[UNIX_PREFIX.length..]
  end

  # Identify this worker to the server: the SDK and the build it is running.
  #
  # The build is Ruby's version rather than a gem version, because a worker is usually
  # vendored rather than installed and a version constant would be one more thing to keep
  # true by hand. `runtime` is a free-form string the server only logs.
  def self.runtime
    "#{PACKAGE}/#{RUBY_VERSION}"
  end

  # The worker side of the channel.
  class Worker
    attr_reader :last_call_was_known

    # Build a worker on explicit streams, defaulting to stdio.
    #
    # The protocol streams are taken from the real `$stdout` *before* it is rebound, which
    # is why this is a class method rather than a constructor call an application makes
    # after the load-time rebinding has happened.
    def self.create(input = nil, output = nil, error = nil)
      input ||= $stdin
      output ||= defined?(Volucer::CHANNEL_OUT) ? Volucer::CHANNEL_OUT : $stdout
      error ||= $stderr
      input.binmode if input.respond_to?(:binmode)
      output.binmode if output.respond_to?(:binmode)
      new(input, output, error)
    end

    # Build a worker, using the socket transport when the server named one.
    #
    # This is what a worker file should call. With `VOLUCER_WORKER_SOCKET` set the protocol
    # runs over a loopback socket; unset, over stdin/stdout exactly as `create` does.
    #
    # The option exists for one measured reason. On Windows, PHP's stream layer takes ~8 ms
    # median to return from a blocking read on a *pipe* while the same read on a *socket*
    # returns in ~0.09 ms (plan hazard 46). Ruby's own pipe cost differs, but the option is
    # the server's and a worker that ignored it would present as a startup timeout.
    #
    # A worker that cannot reach the address exits non-zero rather than falling back to
    # stdio: the server is waiting for a connection, so a silent fallback would hang.
    def self.connect
      address = Volucer.socket_address
      return create if address.nil?

      begin
        path = Volucer.unix_path(address)
        socket =
          if path
            UNIXSocket.new(path)
          else
            host, _, port = address.rpartition(':')
            Socket.tcp(host.empty? ? '127.0.0.1' : host, Integer(port), connect_timeout: 10)
          end
      rescue StandardError => e
        # The address is the server's, so a failure here is a deployment problem and it has
        # to be legible on stderr — which, on the socket transport, is the only channel the
        # server sees.
        $stderr.write("worker: cannot reach #{address} (#{SOCKET_ENV}): #{e.message}\n")
        $stderr.flush
        exit(1)
      end

      new(socket, socket, $stderr)
    end

    def initialize(input, output, error)
      @in = input
      @out = output
      @err = error
      @checksum = true
      @request_id = UNSOLICITED
      @keep_alive = true
      @stopping = false
      @next_call_id = 0xFFFFFFFF
      @call_handler = nil
      @http_handler = nil
      @job_handler = nil
      @grpc_handler = nil
      @last_call_was_known = true
    end

    # Write a diagnostic line to stderr.
    #
    # Never to the protocol stream: a line there would land inside a frame.
    def diagnose(message)
      @err.write("[volucer] #{message}\n")
      @err.flush
    rescue StandardError
      nil
    end

    # The next frame header, or nil at a clean end of stream.
    #
    # A stream that ends before the first byte is a clean close — that is how the server
    # asks a worker to stop. A stream that ends *part-way* through a header is an error,
    # because treating it as a clean close would hide a truncated frame.
    def read_header
      raw = read_exactly(HEADER_LEN, allow_eof: true)
      return nil if raw.nil?

      Header.decode(raw)
    end

    # Read a frame's payload and verify its checksum.
    #
    # A mismatch means framing has already desynchronised, so this is fatal by construction.
    #
    # A frame that declares NO_CHECKSUM is read without hashing it, but only when this
    # worker actually agreed to that; otherwise the claim is a protocol violation.
    def read_payload(header)
      return ''.b if header.payload_len.zero?

      # Refused *before* reading, which is the point of the limit: a corrupt `payload_len`
      # must not make the reader allocate 4 GiB.
      if header.payload_len > MAX_PAYLOAD_LEN
        raise ProtocolError,
              "a frame declares #{header.payload_len} payload bytes, above the " \
              "#{MAX_PAYLOAD_LEN} limit; refusing it before allocating"
      end

      payload = read_exactly(header.payload_len)

      unless header.declares_no_checksum?
        computed = Crc32c.digest(payload)
        if computed != header.checksum
          raise ProtocolError,
                "payload checksum mismatch on request #{header.request_id}: " +
                  format('expected 0x%08x, computed 0x%08x', header.checksum, computed)
        end
        return payload
      end

      if @checksum
        raise ProtocolError,
              "the server sent a #{header.type_name} frame declaring no checksum, which " \
              'this worker never agreed to'
      end

      payload
    end

    # Write one frame, in full.
    def send_frame(frame)
      @out.write(frame.encode(checksum: @checksum))
      @out.flush
    end

    def send_json(message_type, request_id, value)
      send_frame(Frame.new(message_type, request_id, Volucer.json_bytes(value)))
    end

    # Read the HELLO, answer it, and adopt the checksum agreement.
    #
    # The answer is sent *before* the agreement is adopted, so the HELLO_ACK always carries
    # a real checksum: both ends are still verifying at the moment the agreement is
    # exchanged, and only then does the worker stop hashing.
    def handshake
      offer = false

      loop do
        header = read_header
        raise ProtocolError, 'the server closed the pipe before the handshake' if header.nil?

        payload = read_payload(header)

        case header.message_type
        when MessageType::HELLO
          unless payload.empty?
            hello = Volucer.decode_json(payload, 'a HELLO')
            version = hello.fetch('protocol_version', PROTOCOL_VERSION)
            unless version == PROTOCOL_VERSION
              raise ProtocolError,
                    "the server speaks protocol version #{version}, this SDK speaks " \
                    "#{PROTOCOL_VERSION}"
            end
            # Only the JSON literal `true` counts: a field that switches off an integrity
            # check must not be satisfied by a truthy value.
            offer = hello['no_checksum'] == true
          end
          break

        when MessageType::LOG, MessageType::METRIC, MessageType::EVENT
          next

        else
          raise ProtocolError,
                "expected a HELLO to begin the handshake, got a #{header.type_name}"
        end
      end

      send_json(MessageType::HELLO_ACK, UNSOLICITED, {
                  'protocol_version' => PROTOCOL_VERSION,
                  'php_version' => '',
                  'sapi' => '',
                  'pid' => Process.pid,
                  'runtime' => Volucer.runtime,
                  'no_checksum' => offer
                })

      @checksum = false if offer
    end

    # Handshake, then serve requests until the server closes the channel.
    #
    # The block receives the Request and this worker, and may return a Response, a String,
    # an Array in Rack's `[status, headers, body]` shape, or nil for an empty body.
    #
    # Raising is allowed: the SDK catches it, reports it on stderr, and answers 500.
    def run(&handler)
      @http_handler = handler
      handshake

      until @stopping
        header = read_header
        # Clean EOF: the server closed our input, which is how a worker is asked to stop.
        return if header.nil?

        payload = read_payload(header)
        dispatch(header, payload)
      end
    end

    # Handshake, then handle jobs until the server closes the channel.
    #
    # What the block returns decides the job's fate:
    #
    # * `nil` — **acknowledge.** Done.
    # * `false` — **requeue.** Not done, and it should be handed out again.
    # * a String — **reject**, with that string as the reason.
    #
    # A block that *raises* requeues, and that is the safe direction: a consumer that
    # crashes must not lose the job, and an application that wanted a crashing job dropped
    # can rescue its own exception and return a String — which is the visible, deliberate
    # act this default pushes an application towards.
    def consume(&handler)
      @job_handler = handler
      handshake

      until @stopping
        header = read_header
        return if header.nil?

        payload = read_payload(header)
        dispatch(header, payload)
      end
    end

    # Register what answers a CALL the server makes while a request is in flight.
    #
    # The block receives the decoded call as a Hash with 'name' and 'params', and may
    # return a value, or a Hash with 'ok' in it to refuse it — including
    # `'unknown' => true` when the call may have taken effect, which the server treats
    # differently from a refusal.
    def on_call(&handler)
      @call_handler = handler
    end

    # Register the block for gRPC calls, which arrive as ordinary REQUESTs.
    def on_grpc(&handler)
      @grpc_handler = handler
    end

    # Emit a log record without touching the protocol stream.
    def emit_log(message, level = 'info')
      send_json(MessageType::LOG, UNSOLICITED, {
                  'level' => level,
                  'message' => message,
                  'request_id' => @request_id
                })
    end

    # Emit a metric sample.
    #
    # The name must be a Prometheus metric name — `[a-zA-Z_][a-zA-Z0-9_]*` — because that
    # is what it becomes, and the deployment must declare it in `[metrics.collect]` before
    # the server accepts it.
    def emit_metric(name, value, unit = '')
      send_json(MessageType::METRIC, UNSOLICITED, {
                  'name' => name,
                  'value' => Float(value),
                  'unit' => unit
                })
    end

    # Ask the server for something, while a request is in flight.
    #
    # Only legal during a request: between requests the server's exchange thread is parked
    # on its work channel and nothing drains the pipe, so a call made while idle would
    # block until the request deadline killed the worker.
    def call(name, params = {})
      if @request_id == UNSOLICITED
        raise ProtocolError,
              'a call may only be made while a request is in flight; between requests ' \
              'nothing on the server is reading this channel'
      end

      call_id = @next_call_id
      @next_call_id = call_id == 1 ? 0xFFFFFFFF : call_id - 1

      send_json(MessageType::CALL, call_id, { 'name' => name, 'params' => params })

      loop do
        header = read_header
        raise ProtocolError, 'the server closed the channel while a call was in flight' if header.nil?

        payload = read_payload(header)

        if header.message_type == MessageType::RETURN
          unless header.request_id == call_id
            raise ProtocolError,
                  "a RETURN for call #{header.request_id} arrived while waiting for " \
                  "call #{call_id}"
          end

          answer = Volucer.decode_json(payload, 'a RETURN')
          unless answer['ok']
            @last_call_was_known = !answer['unknown']
            raise ProtocolError.new(answer.fetch('error', 'the call was refused'), fatal: false)
          end

          @last_call_was_known = true
          return answer['result']
        end

        # Anything else arriving mid-call is not part of this exchange. Telemetry is
        # absorbed; a correlated frame is a protocol error, because it means two exchanges
        # are in flight on a channel that carries one.
        next if [MessageType::LOG, MessageType::METRIC, MessageType::EVENT].include?(header.message_type)

        raise ProtocolError,
              "while waiting for a RETURN the server sent a #{header.type_name}, which " \
              'this worker cannot serve'
      end
    end

    # Ask to be replaced after the current unit of work.
    #
    # Reported as `keep_alive = false` in the terminal frame, which is also how a worker
    # that handled a fatal error asks to be replaced.
    def recycle
      @keep_alive = false
    end

    private

    def read_exactly(length, allow_eof: false)
      chunks = +''
      while chunks.bytesize < length
        chunk = @in.read(length - chunks.bytesize)
        if chunk.nil? || chunk.empty?
          return nil if allow_eof && chunks.empty?

          raise ProtocolError,
                "the stream ended after #{chunks.bytesize} of #{length} bytes"
        end
        chunks << chunk
      end
      chunks
    end

    def dispatch(header, payload)
      case header.message_type
      when MessageType::REQUEST
        if @http_handler.nil? && @grpc_handler.nil?
          diagnose(
            'the server sent a REQUEST to a consumer; this worker is configured for the ' \
            'wrong mode, and the request is being refused'
          )
          refuse_request(header)
        else
          serve(header, payload)
        end

      when MessageType::JOB
        if @job_handler.nil?
          diagnose(
            'the server sent a JOB to a worker that serves requests; this worker is ' \
            'configured for the wrong mode, and the job is being refused'
          )
          refuse_job(header, payload)
        else
          handle_job(header, payload)
        end

      when MessageType::PING
        send_frame(Frame.new(MessageType::PONG, UNSOLICITED))

      when MessageType::CONTROL
        handle_control(payload)

      when MessageType::CALL
        answer_call(header, payload)

      when MessageType::LOG, MessageType::METRIC, MessageType::EVENT
        # A peer that echoes telemetry back must not derail the loop.
        diagnose("ignoring an unexpected #{header.type_name} from the server")

      else
        raise ProtocolError,
              "the server sent a #{header.type_name}, which a worker cannot serve"
      end
    end

    def serve(header, payload)
      @request_id = header.request_id
      @keep_alive = true

      metadata = Volucer.decode_json(payload, 'a REQUEST header')
      body = read_body(header.request_id, metadata.fetch('body_len', 0).to_i)

      request = Request.new(
        method: metadata.fetch('method', 'GET').to_s,
        uri: metadata.fetch('uri', '/').to_s,
        protocol: metadata.fetch('protocol', 'HTTP/1.1').to_s,
        headers: pairs(metadata['headers']),
        remote_addr: metadata.fetch('remote_addr', '').to_s,
        script_filename: metadata.fetch('script_filename', '').to_s,
        https: metadata['https'] == true,
        body: body
      )

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      app_status = 0

      begin
        result =
          if request.grpc? && @grpc_handler
            @grpc_handler.call(request, self)
          elsif @http_handler
            @http_handler.call(request, self)
          else
            # A gRPC call with no gRPC handler is UNIMPLEMENTED rather than a 500: a method
            # the application does not implement is one it is entitled to refuse.
            Response.text("Not Implemented\n", 501)
          end
        response = normalise(result)
      rescue StandardError => e
        diagnose("uncaught #{e.class} while serving #{request.uri}: #{e.message}")
        response = Response.text("Internal Server Error\n", 500)
        app_status = 1
      end

      duration_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1_000_000).to_i
      respond(response, app_status, duration_us)
    end

    # Read the request body from the BODY frames that follow a REQUEST.
    #
    # Stops at `body_len` bytes, because the declared length is authoritative.
    def read_body(request_id, body_len)
      return ''.b if body_len.zero?

      body = +''
      while body.bytesize < body_len
        header = read_header
        if header.nil?
          raise ProtocolError,
                "the server closed the pipe after #{body.bytesize} of #{body_len} body bytes"
        end

        payload = read_payload(header)

        unless header.message_type == MessageType::BODY
          raise ProtocolError,
                "expected a BODY while reading the request body, got a #{header.type_name}"
        end

        unless header.request_id == request_id
          raise ProtocolError,
                "a BODY for request #{header.request_id} arrived while reading request " \
                "#{request_id}"
        end

        body << payload

        if header.last? && body.bytesize != body_len
          raise ProtocolError,
                "the last BODY frame arrived after #{body.bytesize} of #{body_len} declared bytes"
        end
      end

      body
    end

    # Write a response: head, body chunks, then the terminal status.
    def respond(response, app_status, duration_us)
      body = response.body

      send_json(MessageType::RESPONSE, @request_id, {
                  'status' => response.status,
                  'headers' => response.headers.map { |name, value| [name.to_s, value.to_s] },
                  'body_len' => body.bytesize
                })

      unless body.empty?
        chunks = (body.bytesize + MAX_BODY_CHUNK - 1) / MAX_BODY_CHUNK
        chunks.times do |index|
          start = index * MAX_BODY_CHUNK
          chunk = body.byteslice(start, MAX_BODY_CHUNK)
          send_frame(Frame.new(
                       MessageType::BODY,
                       @request_id,
                       chunk,
                       index == chunks - 1 ? Flags::LAST : 0
                     ))
        end
      end

      send_json(MessageType::END_FRAME, @request_id, {
                  'app_status' => app_status,
                  'peak_memory_bytes' => peak_memory_bytes,
                  'duration_us' => duration_us,
                  'keep_alive' => @keep_alive
                })

      @stopping = true unless @keep_alive
    end

    # The peak memory this worker reached, in bytes.
    #
    # **Per request rather than per process**, because the two behave differently and the
    # difference decides how often a worker is recycled. Ruby's `GC.stat` reports heap
    # slots rather than resident bytes, so this is the process's RSS — cumulative, and
    # therefore documented as such. See the module docstring.
    def peak_memory_bytes
      Volucer.process_rss_bytes
    end

    # Answer a REQUEST a consumer cannot serve, rather than ignoring it.
    #
    # The server's request path has a deadline and its answer to a missed one is to kill
    # the worker, so silence would turn a one-line configuration mistake into a worker
    # killed once a minute.
    def refuse_request(header)
      @request_id = header.request_id
      respond(Response.text("Not Implemented: this worker is a consumer\n", 501), 1, 0)
    end

    def handle_job(header, payload)
      metadata = Volucer.decode_json(payload, 'a JOB header')
      body = read_body(header.request_id, metadata.fetch('body_len', 0).to_i)

      job = Job.new(
        pipeline: metadata.fetch('pipeline', '').to_s,
        id: metadata.fetch('id', '').to_s,
        task: metadata.fetch('task', '').to_s,
        headers: pairs(metadata['headers']),
        attempts: metadata.fetch('attempts', 0).to_i,
        body: body
      )

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      outcome = 'ack'
      reason = nil

      begin
        result = @job_handler.call(job, self)
        if result == false
          outcome = 'requeue'
        elsif result.is_a?(String)
          outcome = 'reject'
          reason = result
        end
      rescue StandardError => e
        # A crashing consumer must not lose work, and an application that wanted a crashing
        # job dropped can rescue its own exception and return a String.
        diagnose("uncaught #{e.class} while consuming job #{job.id}: #{e.message}")
        outcome = 'requeue'
      end

      duration_us = ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - started) * 1_000_000).to_i
      ack(header.request_id, outcome, reason, duration_us)
    end

    def refuse_job(header, payload)
      metadata = Volucer.decode_json(payload, 'a JOB header')
      body_len = metadata.fetch('body_len', 0).to_i
      # The body still has to be drained, or the next frame is read as a body chunk.
      read_body(header.request_id, body_len) unless body_len.zero?
      ack(header.request_id, 'requeue', 'this worker serves requests, not jobs', 0)
    end

    def ack(request_id, outcome, reason, duration_us)
      payload = {
        'outcome' => outcome,
        'peak_memory_bytes' => peak_memory_bytes,
        'keep_alive' => @keep_alive,
        'duration_us' => duration_us
      }
      payload['reason'] = reason unless reason.nil?
      send_json(MessageType::ACK, request_id, payload)
    end

    def answer_call(header, payload)
      call = Volucer.decode_json(payload, 'a CALL')

      begin
        if @call_handler.nil?
          send_json(MessageType::RETURN, header.request_id, {
                      'ok' => false,
                      'error' => 'this worker registered no call handler, so ' \
                                 "#{call.fetch('name', 'the call')} cannot be served"
                    })
          return
        end

        result = @call_handler.call(call)

        # A handler that wants to say anything other than "here is the value" returns a
        # Hash with `ok` in it. Checking for the key rather than for a shape means a result
        # that happens to be a Hash of data is still sent as a result.
        if result.is_a?(Hash) && result.key?('ok')
          send_json(MessageType::RETURN, header.request_id, {
                      'ok' => !!result['ok'],
                      'error' => result['error'],
                      'unknown' => !!result['unknown'],
                      'result' => result['result']
                    })
          return
        end

        send_json(MessageType::RETURN, header.request_id, { 'ok' => true, 'result' => result })
      rescue StandardError => e
        diagnose("a call handler raised #{e.class}: #{e.message}")
        send_json(MessageType::RETURN, header.request_id, {
                    'ok' => false,
                    'error' => "the call handler raised #{e.class}",
                    # Deliberately not unknown: the handler raised before returning, and
                    # whether its side effects happened is the handler's business to report.
                    'unknown' => false
                  })
      end
    end

    def handle_control(payload)
      control = Volucer.decode_json(payload, 'a CONTROL')
      kind = control['kind'].is_a?(String) ? control['kind'] : ''

      send_frame(Frame.new(MessageType::CONTROL_ACK, UNSOLICITED))

      case kind
      when 'drain', 'shutdown'
        # We are idle between requests, so this is immediate.
        @stopping = true
      when 'report'
        emit_metric('worker_peak_memory_bytes', peak_memory_bytes.to_f, 'bytes')
      when 'reload'
        diagnose('reload acknowledged: this worker has no separate bootstrap')
      else
        diagnose("unknown control kind #{kind.inspect} acknowledged and ignored")
      end
    end

    def pairs(value)
      return [] unless value.is_a?(Array)

      value.filter_map do |pair|
        next unless pair.is_a?(Array) && pair.length == 2

        [pair[0].to_s, pair[1].to_s]
      end
    end

    # Turn whatever the block returned into a Response.
    #
    # The Array form is Rack's `[status, headers, body]`, because that is what a framework
    # adapter naturally produces and converting at the boundary is cheaper than making
    # every adapter build the SDK's type.
    def normalise(result)
      case result
      when Response
        result
      when Array
        status, headers, body = result
        text = body.respond_to?(:each) ? body.each_with_object(+'') { |chunk, out| out << chunk } : body.to_s
        Response.new(status.to_i, normalise_headers(headers), text)
      when nil
        Response.html('')
      when String
        Response.html(result)
      else
        raise ProtocolError,
              'a worker block must return a Response, a String, a [status, headers, body] ' \
              "array or nil, got #{result.class}"
      end
    end

    def normalise_headers(headers)
      return [] unless headers.respond_to?(:each)

      collected = []
      headers.each do |pair|
        name, value = pair
        if value.is_a?(Array)
          # Rack 3 allows an Array value for a header that may repeat; two cookies are two
          # headers, and joining them would silently break a session.
          value.each { |one| collected << [name.to_s, one.to_s] }
        else
          collected << [name.to_s, value.to_s]
        end
      end
      collected
    end
  end

  # The process's resident set size, or 0 when the platform will not say.
  #
  # Zero is the protocol's "this runtime does not report it", and the pool must not recycle
  # on a value it did not get — so an honest zero is better than a guess.
  def self.process_rss_bytes
    if Gem.win_platform?
      # `GetProcessMemoryInfo` is the only portable-enough answer on Windows; Ruby has no
      # built-in for it, and shelling out per request would cost more than the number is
      # worth. `WorkingSetSize` from the process's own counters is the figure Task Manager
      # shows, which is the one an operator compares against `memory_limit_mb`.
      require 'fiddle'

      kernel32 = Fiddle.dlopen('kernel32')
      get_current_process = Fiddle::Function.new(
        kernel32['GetCurrentProcess'], [], Fiddle::TYPE_VOIDP
      )
      get_memory_info = Fiddle::Function.new(
        kernel32['K32GetProcessMemoryInfo'],
        [Fiddle::TYPE_VOIDP, Fiddle::TYPE_VOIDP, Fiddle::TYPE_UINT32],
        Fiddle::TYPE_INT
      )

      # PROCESS_MEMORY_COUNTERS is 10 DWORD/pointer-sized fields; 64 bytes is enough on a
      # 32-bit build and 96 on a 64-bit one, so it is allocated generously.
      buffer = Fiddle::Pointer.malloc(96)
      return 0 if get_memory_info.call(get_current_process.call, buffer, 96).zero?

      # Field order: cb, PageFaultCount, PeakWorkingSetSize, WorkingSetSize, …
      buffer[8, 8].unpack1('Q') || 0
    else
      statm = File.read('/proc/self/statm').split
      Integer(statm[1]) * Integer(`getconf PAGESIZE`.strip)
    end
  rescue StandardError
    0
  end
end
