# frozen_string_literal: true

# The request and response objects a Ruby worker's handler sees.
#
# These are the SDK's own types rather than Rack's, because the SDK must work with every
# Ruby web stack and none of them owns the wire. `Volucer::Rack.env` turns a Request into a
# Rack environment; a framework adapter is that environment, not these objects.

require 'uri'

module Volucer
  # One request, as it arrived on the channel.
  #
  # `headers` is a list of pairs rather than a Hash, because the same name may arrive twice
  # and the order is the client's. `#header` is the ordered lookup and `#header_map` is the
  # collapsed form a Rack environment needs.
  class Request
    attr_reader :method, :uri, :protocol, :headers, :remote_addr, :script_filename, :body
    attr_reader :https

    def initialize(
      method: 'GET', uri: '/', protocol: 'HTTP/1.1', headers: [], remote_addr: '',
      script_filename: '', https: false, body: +''
    )
      @method = method
      @uri = uri
      @protocol = protocol
      @headers = headers
      @remote_addr = remote_addr
      @script_filename = script_filename
      @https = https
      @body = body
    end

    def https?
      @https
    end

    # The first value of a header, case-insensitively, or nil.
    def header(name)
      lowered = name.downcase
      @headers.each { |key, value| return value if key.downcase == lowered }
      nil
    end

    # Every header as a name => value map, repeated names joined with a comma.
    #
    # Repeated names are joined rather than overwritten, because that is what a repeated
    # HTTP field means. `Set-Cookie` is the exception and it is a *response* header, so the
    # response side keeps the pair list instead.
    def header_map
      @headers.each_with_object({}) do |(key, value), collapsed|
        collapsed[key] = collapsed.key?(key) ? "#{collapsed[key]}, #{value}" : value
      end
    end

    # The path, without the query string.
    def path
      @uri.split('?', 2).first
    end

    # Everything after the first `?`, or an empty string.
    def query_string
      _, _, query = @uri.partition('?')
      query
    end

    # The query string as a Hash, later values winning.
    def query
      URI.decode_www_form(query_string).to_h
    rescue ArgumentError
      {}
    end

    def content_type
      header('content-type')
    end

    # Whether this request is a gRPC call.
    #
    # The media type decides, on both sides of the channel — see `docs/worker-protocol.md`.
    # A worker that serves both HTTP routes and gRPC services is the ordinary shape, so this
    # is the question an adapter asks rather than something the loop decides for it.
    def grpc?
      media_type = (content_type || '').strip.downcase
      return false unless media_type.start_with?('application/grpc')

      rest = media_type['application/grpc'.length..] || ''
      rest.empty? || [';', '+', '-'].include?(rest[0])
    end
  end

  # A response the SDK will send.
  #
  # Header order is preserved as given, because the server relays it verbatim and two
  # `Set-Cookie` headers are two cookies.
  class Response
    attr_accessor :status, :headers, :body

    def initialize(status = 200, headers = [], body = +'')
      @status = status
      @headers = headers
      @body = body.b
    end

    def self.text(body, status = 200)
      new(status, [['Content-Type', 'text/plain; charset=utf-8']], body)
    end

    def self.html(body, status = 200)
      new(status, [['Content-Type', 'text/html; charset=utf-8']], body)
    end

    def self.json(value, status = 200)
      new(status, [['Content-Type', 'application/json']], JSON.generate(value))
    end

    def with_header(name, value)
      @headers << [name, value]
      self
    end

    def with_status(status)
      @status = status
      self
    end

    def with_body(body)
      @body = body.b
      self
    end
  end

  # One unit of queued work, as a consumer sees it.
  #
  # A job is a separate exchange from a request rather than a flag on one, because the two
  # differ in what an application may assume: a request handler is expected to reset what
  # the last one left behind, and a consumer is expected to keep its connection pool and
  # warm caches across jobs.
  class Job
    attr_reader :pipeline, :id, :task, :headers, :attempts, :body

    def initialize(pipeline: '', id: '', task: '', headers: [], attempts: 0, body: +'')
      @pipeline = pipeline
      @id = id
      @task = task
      @headers = headers
      @attempts = attempts
      @body = body
    end
  end
end
