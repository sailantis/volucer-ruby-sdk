# frozen_string_literal: true

# A Rack application that works as a volucer worker.
#
# The smallest useful shape: a plain Rack callable, served by this SDK. A backend names it:
#
#   [backends.ruby]
#   kind = "worker"
#   command = "ruby examples/rack_app.rb"
#
# The application is deliberately boring — it echoes the request back as JSON — because the
# interesting part of this file is the last line, which is the whole of what makes a Rack
# application a worker.

$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))

require 'json'
require 'volucer'

APP = lambda do |env|
  if env['PATH_INFO'] == '/health'
    [200, { 'Content-Type' => 'text/plain' }, ['ok']]
  else
    body = JSON.generate(
      'method' => env['REQUEST_METHOD'],
      'path' => env['PATH_INFO'],
      'query' => env['QUERY_STRING'],
      'scheme' => env['rack.url_scheme'],
      'host' => env['SERVER_NAME'],
      'body_len' => env['rack.input'].read.to_s.bytesize,
      'accept' => env['HTTP_ACCEPT'].to_s
    )
    [200, { 'Content-Type' => 'application/json', 'Content-Length' => body.bytesize.to_s }, [body]]
  end
end

if $PROGRAM_NAME == __FILE__
  Volucer::Worker.connect.run(&Volucer::Rack.handler(APP))
end
