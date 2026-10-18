# frozen_string_literal: true

# A Rack adapter: turn a `Volucer::Request` into a Rack environment.
#
# This is the half of a Ruby worker that is genuinely new work rather than a transliteration
# of the PHP SDK. PHP has a SAPI that builds `$_SERVER` from a CGI environment, so the PHP
# SDK already receives one; Rack is *also* CGI-derived but has its own opinions, and two of
# them are where a naive adapter goes wrong:
#
# * `rack.input` has to respond to `gets`, `each`, `read` and `rewind`. A Rack app, and
#   `Rack::Lint`, will use all four.
# * `rack.errors` has to be a stream. Pointing it at the protocol stream would corrupt the
#   framing the first time an application logged a warning.
#
# `SCRIPT_NAME` is empty and `PATH_INFO` is the whole path, which is the shape a middleware
# stack mounted at the root expects. A framework that needs to route on a prefix does it from
# `PATH_INFO`, exactly as it does under any other Rack server.

require 'stringio'

module Volucer
  module Rack
    # The default server name, when a `Host` header carries none.
    DEFAULT_SERVER_NAME = 'localhost'

    # Split a `Host` header into a name and a port, with the scheme's default filled in.
    #
    # A bracketed IPv6 literal keeps its brackets and its colons; everything else splits on
    # the last colon, which is the only unambiguous rule a host header has.
    def self.split_host(host, https)
      default_port = https ? '443' : '80'
      return [DEFAULT_SERVER_NAME, default_port] if host.nil? || host.strip.empty?

      host = host.strip
      if host.start_with?('[')
        closing = host.index(']')
        unless closing.nil?
          name = host[0..closing]
          rest = host[(closing + 1)..] || ''
          return [name, rest.start_with?(':') && rest.length > 1 ? rest[1..] : default_port]
        end
        return [host, default_port]
      end

      if host.include?(':')
        name, _, port = host.rpartition(':')
        return [name, port] unless name.empty? || port.empty?
      end

      [host, default_port]
    end

    # Build the Rack environment for one request.
    #
    # `server_port` overrides the port derived from the `Host` header, for a deployment whose
    # listener is not the port a client sees. The header is the better source when it is
    # there, so this is an override rather than the default.
    def self.env(request, server_port = nil)
      name, port = split_host(request.header('host'), request.https?)
      port = server_port unless server_port.nil? || server_port.to_s.empty?

      path = decode_path(request.path)

      env = {
        'REQUEST_METHOD' => request.method,
        'SCRIPT_NAME' => '',
        'PATH_INFO' => path,
        'QUERY_STRING' => request.query_string,
        'SERVER_NAME' => name,
        'SERVER_PORT' => port.to_s,
        'SERVER_PROTOCOL' => request.protocol,
        'REMOTE_ADDR' => request.remote_addr,
        'rack.version' => [1, 3],
        'rack.url_scheme' => request.https? ? 'https' : 'http',
        'rack.input' => StringIO.new(request.body),
        'rack.errors' => ErrorsStream.new,
        'rack.multithread' => false,
        'rack.multiprocess' => true,
        'rack.run_once' => false,
        'rack.hijack?' => false
      }

      # Routing metadata, not a file to execute — the protocol's own documentation says so,
      # and a Rack app that opened this path would be a path-traversal bug waiting to happen.
      # Prefixing it keeps nothing from mistaking it for a CGI variable.
      env['volucer.script_filename'] = request.script_filename unless request.script_filename.empty?

      request.headers.each do |key, value|
        upper = key.upcase.tr('-', '_')
        case upper
        when 'CONTENT_TYPE', 'CONTENT_LENGTH'
          env[upper] = value
        else
          env["HTTP_#{upper}"] = value
        end
      end

      # A Rack server is expected to supply this for a body; the request may have arrived
      # without one, and a middleware that read `rack.input` to the end would otherwise not
      # know how much to expect.
      if !env.key?('CONTENT_LENGTH') && !request.body.empty?
        env['CONTENT_LENGTH'] = request.body.bytesize.to_s
      end

      env
    end

    # Percent-decode a URL path, leaving it as the bytes a Rack app expects.
    def self.decode_path(raw)
      decoded = raw.gsub(/%([0-9A-Fa-f]{2})/) { [Regexp.last_match(1)].pack('H2') }
      decoded.force_encoding(Encoding::UTF_8)
      # A path that is not valid UTF-8 is still a path a request can carry, so it stays as
      # bytes rather than raising. Rack apps that care will inspect `PATH_INFO` themselves.
      decoded.valid_encoding? ? decoded : decoded.dup.force_encoding(Encoding::BINARY)
    end

    # `rack.errors`, writing somewhere that is not the protocol channel.
    #
    # `$stderr` is free (the supervisor drains it), so this is a thin pass-through. An
    # application's warning is worth seeing, and losing it to a full pipe would be the worst
    # of both.
    class ErrorsStream
      def write(text)
        $stderr.write(text)
        text.to_s.length
      end

      def <<(text)
        write(text)
        self
      end

      def puts(*lines)
        lines = [''] if lines.empty?
        lines.each { |line| write("#{line}\n") }
        nil
      end

      def flush
        $stderr.flush
        self
      end

      def sync
        true
      end

      def sync=(value); end

      def close; end
    end

    # Wrap a Rack application so it can be handed to `Worker#run`.
    #
    # The usual shape of a Ruby worker, which is why this exists as a function rather than as
    # documentation:
    #
    #   require 'volucer'
    #   require 'my_app'
    #
    #   Volucer::Worker.connect.run(&Volucer::Rack.handler(MyApp.new))
    #
    # The result is a callable taking `(request, worker)` and returning a `Response`, which
    # is what `run` wants.
    def self.handler(app)
      lambda do |request, _worker|
        status, headers, body = app.call(env(request))

        collected = +''
        begin
          body.each { |chunk| collected << chunk.to_s }
        ensure
          body.close if body.respond_to?(:close)
        end

        Response.new(status.to_i, normalise_headers(headers), collected)
      end
    end

    def self.normalise_headers(headers)
      return [] if headers.nil?

      collected = []
      headers.each do |name, value|
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
end
