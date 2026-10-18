# frozen_string_literal: true

# The Ruby side of the protocol freeze.
#
# Every golden value here is the *same literal* that
# `crates/volucer-worker/tests/wire_format.rs` in `sailantis/volucer`,
# `tests/WireFormatTest.php` in `sailantis/volucer-php-sdk` and
# `tests/test_wire_format.py` in `sailantis/volucer-python-sdk` assert. That is the point: if
# any implementation drifts, one of the four tests fails, and none can quietly become
# "whatever the code currently does".
#
# The fixtures were computed independently of all four implementations, so they are a
# transcript of the specification rather than of any one of them.

require 'minitest/autorun'

require_relative '../lib/volucer/protocol'

class Crc32cTest < Minitest::Test
  def test_the_standard_check_value
    assert_equal 0xE3069283, Volucer::Crc32c.digest('123456789')
  end

  def test_empty_input_is_zero
    assert_equal 0, Volucer::Crc32c.digest('')
  end

  def test_the_golden_response_payload
    assert_equal 0x7B9F1283,
                 Volucer::Crc32c.digest('{"status":200,"headers":[],"body_len":0}')
  end

  def test_the_golden_end_payload
    assert_equal 0xB3E50F77,
                 Volucer::Crc32c.digest(
                   '{"app_status":0,"peak_memory_bytes":0,"duration_us":0,"keep_alive":true}'
                 )
  end

  def test_crc32c_is_not_crc32b
    # Zlib.crc32 is CRC-32B: a different polynomial. Using it would make every frame fail
    # its checksum against a correct peer.
    require 'zlib'
    refute_equal 0xE3069283, Zlib.crc32('123456789') & 0xFFFFFFFF
  end
end

class HeaderLayoutTest < Minitest::Test
  def test_the_canonical_zero_fixture
    assert_equal '00000000010100000000000000000000',
                 Volucer::Frame.new(Volucer::MessageType::HELLO, 0).encode.unpack1('H*')
  end

  def test_a_distinct_value_in_every_field
    raw = [0x11223344, 1, Volucer::MessageType::HELLO, 0x5566, 0x778899AA, 0xBBCCDDEE]
          .pack('VCCvVV')
    assert_equal '4433221101016655aa998877eeddccbb', raw.unpack1('H*')

    header = Volucer::Header.decode(raw)
    assert_equal 0x11223344, header.payload_len
    assert_equal Volucer::MessageType::HELLO, header.message_type
    assert_equal 0x5566, header.flags
    assert_equal 0x778899AA, header.request_id
    assert_equal 0xBBCCDDEE, header.checksum
  end

  def test_a_header_is_exactly_sixteen_bytes
    assert_raises(Volucer::ProtocolError) { Volucer::Header.decode("\x00" * 15) }
  end

  def test_an_unknown_type_is_refused_rather_than_skipped
    raw = [0, 1, 19, 0, 0, 0].pack('VCCvVV')
    assert_raises(Volucer::ProtocolError) { Volucer::Header.decode(raw) }
  end
end

class GoldenExchangeTest < Minitest::Test
  # RESPONSE(status 200, no headers, empty body) then END(keep_alive), byte for byte.
  def test_the_exchange_matches_the_other_three_fixtures
    head = Volucer.json_bytes('status' => 200, 'headers' => [], 'body_len' => 0)
    assert_equal '{"status":200,"headers":[],"body_len":0}', head

    ending = Volucer.json_bytes(
      'app_status' => 0, 'peak_memory_bytes' => 0, 'duration_us' => 0, 'keep_alive' => true
    )
    assert_equal '{"app_status":0,"peak_memory_bytes":0,"duration_us":0,"keep_alive":true}',
                 ending

    exchange = Volucer::Frame.new(Volucer::MessageType::RESPONSE, 1, head).encode +
               Volucer::Frame.new(Volucer::MessageType::END_FRAME, 1, ending).encode

    expected = '28000000' + '01' + '04' + '0000' + '01000000' + '83129f7b' \
      '7b22737461747573223a3230302c2268656164657273223a5b5d2c22626f64795f6c656e223a307d' \
      '48000000' + '01' + '0b' + '0000' + '01000000' + '770fe5b3' \
      '7b226170705f737461747573223a302c227065616b5f6d656d6f72795f6279746573223a302c' \
      '226475726174696f6e5f7573223a302c226b6565705f616c697665223a747275657d'

    assert_equal expected, exchange.unpack1('H*')
    assert_equal 144, exchange.bytesize
  end
end

class MessageTypeTest < Minitest::Test
  FROZEN = [
    [Volucer::MessageType::HELLO, 1],
    [Volucer::MessageType::HELLO_ACK, 2],
    [Volucer::MessageType::REQUEST, 3],
    [Volucer::MessageType::RESPONSE, 4],
    [Volucer::MessageType::BODY, 5],
    [Volucer::MessageType::EVENT, 6],
    [Volucer::MessageType::LOG, 7],
    [Volucer::MessageType::METRIC, 8],
    [Volucer::MessageType::CONTROL, 9],
    [Volucer::MessageType::CONTROL_ACK, 10],
    [Volucer::MessageType::END_FRAME, 11],
    [Volucer::MessageType::PING, 12],
    [Volucer::MessageType::PONG, 13],
    [Volucer::MessageType::ERROR, 14],
    [Volucer::MessageType::CALL, 15],
    [Volucer::MessageType::RETURN, 16],
    [Volucer::MessageType::JOB, 17],
    [Volucer::MessageType::ACK, 18]
  ].freeze

  def test_the_numbering_is_the_protocol
    FROZEN.each do |type, number|
      encoded = Volucer::Frame.new(type, 0).encode
      assert_equal number, encoded.getbyte(5), Volucer::MessageType.name(number)
      assert_equal number, type
    end
  end
end

class FlagsTest < Minitest::Test
  def test_a_compressed_frame_is_refused
    raw = [0, 1, Volucer::MessageType::REQUEST, Volucer::Flags::COMPRESSED, 1, 0].pack('VCCvVV')
    assert_raises(Volucer::ProtocolError) { Volucer::Header.decode(raw) }
  end

  def test_no_checksum_writes_zero_and_raises_the_flag
    encoded = Volucer::Frame.new(Volucer::MessageType::BODY, 1, 'hello').encode(checksum: false)
    header = Volucer::Header.decode(encoded[0, 16])
    assert header.declares_no_checksum?
    assert_equal 0, header.checksum
    assert_equal 'hello', encoded[16..]
  end

  def test_zero_is_a_real_checksum_so_the_flag_is_not_zero
    # The reason NO_CHECKSUM is a flag: crc32c('') == 0, which every empty-payload frame
    # carries, so a zero sentinel would make a correctly-checksummed HELLO indistinguishable
    # from an unchecked one.
    header = Volucer::Header.decode(Volucer::Frame.new(Volucer::MessageType::HELLO, 0).encode[0, 16])
    assert_equal 0, header.checksum
    refute header.declares_no_checksum?
  end
end
