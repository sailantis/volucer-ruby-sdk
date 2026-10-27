# frozen_string_literal: true

# A Ruby worker SDK for volucer's worker protocol.
#
# The specification is `docs/worker-protocol.md` in `sailantis/volucer`; this package is the
# other end of it, beside the PHP one in `sailantis/volucer-php-sdk` and the Python one in
# `sailantis/volucer-python-sdk`.
#
# The ordinary use is three lines:
#
#   require 'volucer'
#   require 'my_app'                      # a Rack application
#
#   Volucer::Worker.connect.run(&Volucer::Rack.handler(MyApp.new))
#
# Requiring this file **reassigns `$stdout` to `$stderr`**, and that is deliberate rather
# than surprising once the reason is known: the worker protocol uses the child process's
# stdout as its channel, so anything an application `puts` would land inside a frame and
# desynchronise the stream permanently. The PHP SDK solves the same problem with an output
# buffer and the Python SDK by rebinding `sys.stdout`; this is Ruby's version of the same
# decision. The real channel is kept in `Volucer::CHANNEL_OUT` and `Volucer::CHANNEL_IN`.

require_relative 'volucer/version'
require_relative 'volucer/protocol'
require_relative 'volucer/message'
require_relative 'volucer/grpc'
require_relative 'volucer/rpc'
require_relative 'volucer/services'
require_relative 'volucer/worker'
require_relative 'volucer/rack'
require_relative 'volucer/supervisor'

module Volucer
  # The protocol channel, captured before `$stdout` is reassigned.
  CHANNEL_OUT = $stdout

  # The protocol channel, captured before `$stdin` could be wrapped by anything.
  CHANNEL_IN = $stdin

  # A `$stdout` that cannot corrupt the worker channel.
  #
  # Writes go to `$stderr`, which is not part of the protocol and is drained by the
  # supervisor. An application's `puts` therefore still reaches a human, just not the wire.
  class GuardedStdout
    def initialize(err = $stderr)
      @err = err
    end

    def write(*objects)
      written = 0
      objects.each { |object| written += @err.write(object.to_s) }
      written
    end

    def <<(object)
      @err.write(object.to_s)
      self
    end

    def print(*objects)
      objects.each { |object| @err.write(object.to_s) }
      nil
    end

    def printf(format, *args)
      @err.write(format(format, *args))
      nil
    end

    def puts(*objects)
      @err.puts(*objects)
      nil
    end

    def flush
      @err.flush
      self
    end

    def sync
      true
    end

    def sync=(value); end

    def tty?
      false
    end

    def isatty
      false
    end

    def fileno
      # Deliberately the *real* stderr descriptor rather than stdout's, so a library that
      # does fd-level writes lands in the same place as everything else.
      @err.fileno
    end

    def close; end

    def closed?
      false
    end
  end
end

# Reassign `$stdout`, keeping the real channel in `Volucer::CHANNEL_OUT`. Done at load rather
# than at worker construction, because construction reads the constant and the two must not
# be able to be confused.
$stdout = Volucer::GuardedStdout.new($stderr) unless $stdout.is_a?(Volucer::GuardedStdout)
