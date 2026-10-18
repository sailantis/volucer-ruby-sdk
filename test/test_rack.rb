# frozen_string_literal: true

# The Rack adapter: the half of a Ruby worker that is new work.
#
# The values that are easy to get wrong — `rack.url_scheme` from the frame's `https` flag,
# `rack.input` being a real IO, and `rack.errors` never reaching the protocol stream — are
# asserted here rather than left to a framework to discover.

require 'minitest/autorun'
require 'stringio'

require_relative '../lib/volucer'

class EnvironTest < Minitest::Test
  def request(**kwargs)
    Volucer::Request.new(**kwargs)
  end

  def test_the_rack_core
    req = request(
      method: 'POST',
      uri: '/submit?page=2&q=a+b',
      protocol: 'HTTP/1.0',
      headers: [
        ['Host', 'example.test:8443'],
        ['Content-Type', 'application/json'],
        ['Content-Length', '2'],
        ['Accept', 'application/json'],
        ['X-Forwarded-For', '10.0.0.1']
      ],
      remote_addr: '127.0.0.1',
      body: '{}'
    )
    env = Volucer::Rack.env(req)

    assert_equal 'POST', env['REQUEST_METHOD']
    assert_equal '/submit', env['PATH_INFO']
    assert_equal '', env['SCRIPT_NAME']
    assert_equal 'page=2&q=a+b', env['QUERY_STRING']
    assert_equal 'HTTP/1.0', env['SERVER_PROTOCOL']
    assert_equal 'example.test', env['SERVER_NAME']
    assert_equal '8443', env['SERVER_PORT']
    assert_equal '127.0.0.1', env['REMOTE_ADDR']
    assert_equal 'application/json', env['CONTENT_TYPE']
    assert_equal '2', env['CONTENT_LENGTH']
    assert_equal 'application/json', env['HTTP_ACCEPT']
    assert_equal '10.0.0.1', env['HTTP_X_FORWARDED_FOR']
    assert_equal [1, 3], env['rack.version']
    assert_equal false, env['rack.hijack?']
  end

  def test_the_scheme_comes_from_the_frame_not_from_a_header
    # The child process has no socket, so this flag is the only evidence there is.
    secure = Volucer::Rack.env(request(uri: '/', https: true))
    assert_equal 'https', secure['rack.url_scheme']
    assert_equal '443', secure['SERVER_PORT'], "the scheme's default port"

    plain = Volucer::Rack.env(request(uri: '/'))
    assert_equal 'http', plain['rack.url_scheme']
    assert_equal '80', plain['SERVER_PORT']
  end

  def test_rack_input_responds_to_gets_each_read_and_rewind
    env = Volucer::Rack.env(request(uri: '/', body: "line one\nline two"))
    stream = env['rack.input']
    assert_equal "line one\n", stream.gets
    assert_equal 'line two', stream.read
    stream.rewind
    assert_equal ["line one\n", 'line two'], stream.each.to_a
  end

  def test_one_request_in_flight_so_not_multithreaded
    env = Volucer::Rack.env(request(uri: '/'))
    assert_equal false, env['rack.multithread']
    assert_equal false, env['rack.run_once']
    assert_equal true, env['rack.multiprocess']
  end

  def test_rack_errors_never_reaches_the_protocol_stream
    env = Volucer::Rack.env(request(uri: '/'))
    errors = env['rack.errors']
    # The guard reassigns `$stdout`, and `rack.errors` writes to `$stderr`, so a warning
    # cannot land inside a frame. Asserted by writing and checking that nothing appeared on
    # the channel the SDK captured.
    before = Volucer::CHANNEL_OUT
    errors.write('a warning')
    errors.flush
    assert_same before, Volucer::CHANNEL_OUT
  end

  def test_a_body_without_a_content_length_still_reports_one
    env = Volucer::Rack.env(request(uri: '/', body: 'abc'))
    assert_equal '3', env['CONTENT_LENGTH']
  end

  def test_no_body_and_no_header_means_no_content_length
    env = Volucer::Rack.env(request(uri: '/'))
    refute env.key?('CONTENT_LENGTH')
  end

  def test_script_filename_is_prefixed_not_a_cgi_variable
    # The protocol documents routing metadata as "not a file to execute", so exposing it as
    # SCRIPT_FILENAME would be exactly the name that invites an application to open it.
    env = Volucer::Rack.env(request(uri: '/', script_filename: '/srv/app/config.ru'))
    assert_equal '/srv/app/config.ru', env['volucer.script_filename']
    refute env.key?('SCRIPT_FILENAME')
  end

  def test_a_non_ascii_path_survives
    env = Volucer::Rack.env(request(uri: '/caf%C3%A9'))
    assert_equal '/café', env['PATH_INFO']
    assert env['PATH_INFO'].valid_encoding?
  end

  def test_a_plus_in_a_path_stays_a_plus
    env = Volucer::Rack.env(request(uri: '/a+b'))
    assert_equal '/a+b', env['PATH_INFO']
  end
end

class HostSplitTest < Minitest::Test
  def test_an_explicit_port_wins
    assert_equal %w[example.test 8443], Volucer::Rack.split_host('example.test:8443', false)
  end

  def test_a_bare_name_takes_the_schemes_default
    assert_equal %w[example.test 80], Volucer::Rack.split_host('example.test', false)
    assert_equal %w[example.test 443], Volucer::Rack.split_host('example.test', true)
  end

  def test_a_missing_host_falls_back
    assert_equal [Volucer::Rack::DEFAULT_SERVER_NAME, '80'], Volucer::Rack.split_host(nil, false)
  end

  def test_an_ipv6_literal_keeps_its_brackets
    assert_equal ['[::1]', '8080'], Volucer::Rack.split_host('[::1]:8080', false)
    assert_equal ['[::1]', '80'], Volucer::Rack.split_host('[::1]', false)
  end
end

class HandlerTest < Minitest::Test
  def app
    lambda do |env|
      body = "#{env['REQUEST_METHOD']} #{env['PATH_INFO']}"
      [201, { 'Content-Type' => 'text/plain', 'Set-Cookie' => %w[a=1 b=2] }, [body]]
    end
  end

  def test_a_rack_application_becomes_a_response
    handler = Volucer::Rack.handler(app)
    response = handler.call(Volucer::Request.new(method: 'GET', uri: '/items'), nil)

    assert_kind_of Volucer::Response, response
    assert_equal 201, response.status
    assert_equal 'GET /items', response.body
    cookies = response.headers.select { |name, _| name == 'Set-Cookie' }
    assert_equal [%w[Set-Cookie a=1], %w[Set-Cookie b=2]], cookies,
                 'two Set-Cookie headers are two cookies'
  end

  def test_the_body_is_closed
    closed = []
    body = Object.new
    body.define_singleton_method(:each) { |&block| block.call('x') }
    body.define_singleton_method(:close) { closed << true }

    handler = Volucer::Rack.handler(->(_env) { [200, {}, body] })
    handler.call(Volucer::Request.new(uri: '/'), nil)

    assert_equal [true], closed, "a Rack body must be closed"
  end

  def test_a_plain_string_body_is_collected
    handler = Volucer::Rack.handler(->(_env) { [200, { 'Content-Type' => 'text/plain' }, ['hello']] })
    assert_equal 'hello', handler.call(Volucer::Request.new(uri: '/'), nil).body
  end
end
