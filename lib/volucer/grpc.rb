# frozen_string_literal: true

# The outcome of one gRPC call.
#
# A gRPC reply is not an HTTP response, and modelling it as one is the mistake this class
# exists to prevent. Its **outcome travels in the trailer section** — after the body, not in
# the status line — because a streaming reply has to be able to send headers before its
# outcome exists. So a handler says what happened by naming a status here, and the server
# moves it to where a client reads it.
#
# `payload` is the **body**: the raw, length-delimited framed bytes the client receives. The
# server never parses or re-frames it, so a handler owns the framing entirely. This SDK does
# not ship an encoder, because doing so would put a schema in the transport.

module Volucer
  class GrpcResponse
    OK = 0
    CANCELLED = 1
    UNKNOWN = 2
    INVALID_ARGUMENT = 3
    DEADLINE_EXCEEDED = 4
    NOT_FOUND = 5
    ALREADY_EXISTS = 6
    PERMISSION_DENIED = 7
    RESOURCE_EXHAUSTED = 8
    FAILED_PRECONDITION = 9
    ABORTED = 10
    OUT_OF_RANGE = 11
    UNIMPLEMENTED = 12
    INTERNAL = 13
    UNAVAILABLE = 14
    DATA_LOSS = 15
    UNAUTHENTICATED = 16

    NAMES = {
      OK => 'OK', CANCELLED => 'CANCELLED', UNKNOWN => 'UNKNOWN',
      INVALID_ARGUMENT => 'INVALID_ARGUMENT', DEADLINE_EXCEEDED => 'DEADLINE_EXCEEDED',
      NOT_FOUND => 'NOT_FOUND', ALREADY_EXISTS => 'ALREADY_EXISTS',
      PERMISSION_DENIED => 'PERMISSION_DENIED',
      RESOURCE_EXHAUSTED => 'RESOURCE_EXHAUSTED',
      FAILED_PRECONDITION => 'FAILED_PRECONDITION', ABORTED => 'ABORTED',
      OUT_OF_RANGE => 'OUT_OF_RANGE', UNIMPLEMENTED => 'UNIMPLEMENTED',
      INTERNAL => 'INTERNAL', UNAVAILABLE => 'UNAVAILABLE', DATA_LOSS => 'DATA_LOSS',
      UNAUTHENTICATED => 'UNAUTHENTICATED'
    }.freeze

    attr_reader :status, :payload, :reason, :content_type

    def initialize(status = OK, payload = +'', reason = '', content_type = 'application/grpc+proto')
      @status = status
      @payload = payload.b
      @reason = reason
      @content_type = content_type
    end

    # A successful call carrying `payload` — the framed bytes a client will decode.
    def self.message(payload, content_type = 'application/grpc+proto')
      new(OK, payload, '', content_type)
    end

    # A successful call with no payload.
    def self.ok(content_type = 'application/grpc+proto')
      new(OK, +'', '', content_type)
    end

    # A failed call, with `reason` as the sentence a developer will read.
    #
    # `reason` is **not** payload bytes, and there are none: a failed call carries no
    # message. The server percent-encodes the sentence into the `grpc-message` trailer, so
    # it may contain spaces, quotes and newlines.
    def self.fail(status, reason = '')
      new(status, +'', reason)
    end

    def ok?
      @status == OK
    end

    def status_name
      NAMES.fetch(@status, '')
    end
  end
end
