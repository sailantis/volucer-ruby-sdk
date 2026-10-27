# frozen_string_literal: true

# The volucer worker protocol, as a Ruby client implements it.
#
# The specification is `docs/worker-protocol.md` in `sailantis/volucer`; this module is
# the other end of it, beside the PHP SDK in `sailantis/volucer-php-sdk` and the Python one
# in `sailantis/volucer-python-sdk`. All three implement the same document rather than each
# other, and the wire-format tests assert the same frozen bytes.
#
# Everything here is stdlib-only, deliberately: a worker SDK is loaded before an
# application's own gems are known to work, and a protocol codec that needs a gem is one
# that can fail for a reason unrelated to the protocol.

require 'json'

module Volucer
  # The only protocol version that exists so far.
  PROTOCOL_VERSION = 1

  # The size of a frame header on the wire.
  HEADER_LEN = 16

  # Refuse absurd payloads before allocating, matching MAX_PAYLOAD_LEN in Rust.
  MAX_PAYLOAD_LEN = 64 * 1024 * 1024

  # The largest body slice sent in one frame, matching MAX_BODY_CHUNK in Rust.
  MAX_BODY_CHUNK = 64 * 1024

  # The request id reserved for unsolicited frames.
  UNSOLICITED = 0

  # The message type table. The numbering *is* the protocol: never reorder, never reuse.
  module MessageType
    HELLO = 1
    HELLO_ACK = 2
    REQUEST = 3
    RESPONSE = 4
    BODY = 5
    EVENT = 6
    LOG = 7
    METRIC = 8
    CONTROL = 9
    CONTROL_ACK = 10
    END_FRAME = 11
    PING = 12
    PONG = 13
    ERROR = 14
    CALL = 15
    RETURN = 16
    JOB = 17
    ACK = 18

    NAMES = {
      HELLO => 'HELLO', HELLO_ACK => 'HELLO_ACK', REQUEST => 'REQUEST',
      RESPONSE => 'RESPONSE', BODY => 'BODY', EVENT => 'EVENT', LOG => 'LOG',
      METRIC => 'METRIC', CONTROL => 'CONTROL', CONTROL_ACK => 'CONTROL_ACK',
      END_FRAME => 'END', PING => 'PING', PONG => 'PONG', ERROR => 'ERROR',
      CALL => 'CALL', RETURN => 'RETURN', JOB => 'JOB', ACK => 'ACK'
    }.freeze

    # The wire name of a type, for diagnostics.
    def self.name(type)
      NAMES.fetch(type, "unknown (#{type})")
    end
  end

  # Bits in the `flags` field.
  module Flags
    COMPRESSED = 0x0001
    CHUNKED = 0x0002
    LAST = 0x0004
    ERROR = 0x0008
    NO_CHECKSUM = 0x0010
  end

  # A framing or message failure.
  #
  # The names are the protocol's own: a *framing* failure means the reader can no longer
  # trust its position in the stream and the worker must be discarded, while a *message*
  # failure (a wrong request id, a remote error) leaves the stream aligned.
  class ProtocolError < StandardError
    attr_reader :fatal

    # Whether the failed operation may nevertheless have taken effect.
    #
    # The third answer `RETURN` has room for, and the one a retry has to know about: a call
    # that was **written** and never answered -- a broken connection, an expired deadline --
    # may have been served, so retrying it may duplicate a side effect. A refusal the server
    # actually sent is `false`: nothing happened, and retrying is safe.
    #
    # Always `false` on a worker's channel, where the protocol guarantees an answer; it is
    # {Rpc} that can reach the undecided case, because it reads a socket.
    attr_reader :unknown

    def initialize(message, fatal: true, unknown: false)
      super(message)
      @fatal = fatal
      @unknown = unknown
    end

    # Whether the failure may have taken effect anyway, named for the question a caller asks.
    #
    # `may_have_taken_effect?` rather than a predicate over `#unknown`, because the sentence a caller
    # actually needs to answer is "is it safe to retry this?", and `unknown?` reads as "the error is
    # of an unknown kind" to anyone who has not read this file.
    def may_have_taken_effect?
      @unknown
    end

    # The field's own name, kept because `RETURN` spells it `unknown` on the wire.
    def unknown?
      @unknown
    end
  end

  # CRC-32C (Castagnoli).
  #
  # A table rather than `Zlib.crc32`, which is CRC-32B: a different polynomial, so using it
  # would make every frame fail its checksum against a correct peer. The check value is the
  # standard one — `crc32c("123456789") == 0xE3069283` — and the test suite pins it.
  module Crc32c
    POLYNOMIAL = 0x82F63B78

    TABLE = Array.new(256) do |index|
      value = index
      8.times { value = (value & 1).zero? ? value >> 1 : (value >> 1) ^ POLYNOMIAL }
      value
    end.freeze

    def self.digest(data)
      crc = 0xFFFFFFFF
      data.each_byte do |byte|
        crc = TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
      end
      crc ^ 0xFFFFFFFF
    end
  end

  # A decoded frame header: the 16 bytes that precede a payload.
  #
  # A direct transliteration of `Header::decode` in
  # `crates/volucer-worker/src/header.rs`. Layout, little-endian:
  #
  #   offset size field
  #   0      4    payload_len
  #   4      1    protocol_version
  #   5      1    message_type
  #   6      2    flags
  #   8      4    request_id
  #   12     4    crc32c(payload)
  Header = Struct.new(:payload_len, :message_type, :flags, :request_id, :checksum) do
    # Parse a header, which must be exactly 16 bytes.
    #
    # Exactly, not "at least". A stream that ended mid-header is an error rather than a
    # clean close: treating it as one would hide a truncated frame.
    def self.decode(raw)
      length = raw.bytesize
      unless length == HEADER_LEN
        raise ProtocolError, "a frame header is exactly #{HEADER_LEN} bytes, got #{length}"
      end

      payload_len, version, type, flags, request_id, checksum =
        raw.unpack('VCCvVV')

      unless version == PROTOCOL_VERSION
        raise ProtocolError,
              "unsupported protocol version #{version} (this SDK speaks #{PROTOCOL_VERSION})"
      end

      unless type.between?(MessageType::HELLO, MessageType::ACK)
        raise ProtocolError, "unknown message type #{type} (this SDK defines 1..18)"
      end

      # Refused rather than misread: with no codec negotiated, a "compressed" payload would
      # be handed to JSON as if it were plain text.
      unless (flags & Flags::COMPRESSED).zero?
        raise ProtocolError, 'refusing a compressed frame: no codec is negotiated'
      end

      new(payload_len, type, flags, request_id, checksum)
    end

    def last?
      (flags & Flags::LAST) != 0
    end

    # The writer's *claim*, not a permission. Whether it may be believed is the reader's
    # decision, and `Worker#read_payload` refuses one the handshake never agreed to.
    def declares_no_checksum?
      (flags & Flags::NO_CHECKSUM) != 0
    end

    def type_name
      MessageType.name(message_type)
    end
  end

  # A frame ready to send: a message type, a request id and a payload.
  Frame = Struct.new(:message_type, :request_id, :payload, :flags) do
    def initialize(message_type, request_id, payload = '', flags = 0)
      super(message_type, request_id, payload.b, flags)
    end

    # Serialize to header + payload.
    #
    # With the checksum off the field is written as zero and NO_CHECKSUM is raised, which
    # is what makes the zero mean "not a checksum" rather than "the checksum of an empty
    # payload".
    def encode(checksum: true)
      effective_flags = checksum ? flags : (flags | Flags::NO_CHECKSUM)
      value = checksum ? Crc32c.digest(payload) : 0
      [payload.bytesize, PROTOCOL_VERSION, message_type, effective_flags,
       request_id, value].pack('VCCvVV') + payload
    end
  end

  # JSON-encode a payload the way the protocol expects it.
  #
  # `JSON.generate` carries no spaces, which is what the golden fixtures in all three SDKs
  # are written against.
  def self.json_bytes(value)
    JSON.generate(value, ascii_only: false).b
  end

  # Decode a JSON payload, turning a bad payload into a protocol error.
  def self.decode_json(payload, what)
    return {} if payload.nil? || payload.empty?

    decoded = begin
      JSON.parse(payload)
    rescue JSON::ParserError => e
      raise ProtocolError, "#{what} was not valid JSON: #{e.message}"
    end

    unless decoded.is_a?(Hash)
      raise ProtocolError, "#{what} must be a JSON object"
    end

    decoded
  end

  # The extensions, message types and flags are loaded by `volucer/worker`; requiring this
  # file alone gives a caller the codec without a process.
end
