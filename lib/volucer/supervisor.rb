# frozen_string_literal: true

# A prefork supervisor for volucer's `attach` backend.
#
# The other half of `attach`. volucer only *dials* a relay in this mode -- it starts no
# process at all -- because the fork has to happen in the application's own process: a PHP
# process cannot pass a file descriptor to another process (`ext/sockets` has no
# `SCM_RIGHTS`), so the side that owns the listening socket has to be the side that owns the
# fork. The same is true of the Ruby SDK, for a different reason: `fork` in a process that
# has already started threads is a question with no good answer, and a library cannot know
# when that is. So the fork belongs in the supervisor, and the supervisor belongs to the
# application.
#
#   require 'volucer'
#   require_relative 'config/environment'   # boot the app first
#
#   Volucer::Supervisor.serve(
#     'unix:/run/app/workers.sock',
#     Volucer::Rack.handler(Rails.application),
#     children: 16
#   )
#
# **Unix only.** `Process.fork` does not exist on Windows (and JRuby has it disabled by
# default), and `serve` raises there rather than silently degrading to one process. On
# Windows use `kind = "worker"` (volucer spawns the processes) or `kind = "upstream"`.
#
# **Boot before the fork.** Whatever the handler closes over is inherited by every child --
# a Rails application, a connection-pool *configuration*, loaded classes -- so it has to be
# built before calling `serve`. What must *not* be built before the fork is an open
# connection: every child would inherit it and they would take turns reading from it. Rails
# is safe here because `ActiveRecord` checks connections out lazily, per request, and
# `ActionDispatch::Executor` returns them to the pool afterwards.
#
# **`Process.detach` is not optional.** A child that exits stays a zombie until its parent
# waits for it, so a supervisor that only accepts and forks grows one per connection --
# measured at exactly 10 zombies for 10 connections on the PHP side.
require 'socket'

module Volucer
  module Supervisor
    # The `unix:` prefix volucer's configuration uses for a path relay.
    UNIX_PREFIX = 'unix:'

    # The relay to listen on: the argument, else the environment, else nil.
    #
    # nil rather than an exception, because whether a missing relay is an error depends on the
    # caller. `serve` turns it into a startup failure naming both places; a test or an embedding
    # application may want to decide for itself.
    #
    # The trimming rule is `Volucer.socket_address`'s, deliberately: an empty value is what a shell
    # produces from `VOLUCER_RELAY_SOCKET=`, and reporting it as an empty address would send the
    # operator looking for a typo in a file that never mentions the key.
    def self.relay(relay = nil)
      unless relay.nil?
        relay = relay.strip
        return relay unless relay.empty?
      end

      address = ENV.fetch(RELAY_ENV, nil)
      return nil if address.nil?

      address = address.strip
      address.empty? ? nil : address
    end

    # How long `accept` waits before the master reaps children and looks again.
    #
    # It costs nothing when traffic is arriving -- `accept` returns the instant a connection
    # is pending -- and bounds how long the master sleeps when none is, which is exactly when
    # it needs to be reaping.
    ACCEPT_TIMEOUT = 0.5

    # How long to wait when every child slot is busy.
    SLOT_BACKOFF = 0.005

    # The filesystem path of a `unix:` relay, or nil for a TCP one.
    def self.relay_path(relay)
      relay = relay.strip
      return nil unless relay.start_with?(UNIX_PREFIX)

      relay[UNIX_PREFIX.length..]
    end

    # Bind and listen on a relay, removing a stale socket file first.
    #
    # A unix socket path is a *file*, and binding one that already exists fails with
    # `EADDRINUSE` -- so a supervisor restarted after a crash could never come up. Unlinking
    # first is what every Unix server does, and it is safe here because this process is meant
    # to be the only listener on that path: a live supervisor would hold the socket, and the
    # bind would fail rather than two processes sharing one relay.
    def self.listen(relay)
      path = relay_path(relay)
      if path.nil?
        host, _, port = relay.rpartition(':')
        listener = TCPServer.new(host.empty? ? '127.0.0.1' : host, Integer(port))
        listener.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
        return listener
      end

      begin
        File.unlink(path)
      rescue Errno::ENOENT
        nil
      end

      server = UNIXServer.new(path)
      server
    end

    # Listen on `relay` and fork one child per accepted connection.
    #
    # `handler` is the same block `Worker#run` takes, so a Rack application is
    # `Volucer::Rack.handler(application)`.
    #
    # `children` caps how many children may run at once, not a queue depth. `nil` — the
    # default — means **no cap**, which is what an `attach` deployment wants: volucer dials the
    # relay exactly `workers` times at startup and holds one connection per slot, so the live
    # children are already bounded by that number. A cap is only useful to reserve memory for
    # *other* processes on the host, and a cap **below** `workers` is not a queue -- it is a
    # startup failure, because the surplus dials sit unaccepted in the listen backlog while the
    # pool waits for a handshake no child is there to send.
    #
    # Each child serves exactly one connection and exits, which is what makes recycling cheap
    # and the assignment exact: volucer closes a slot, the child sees end-of-file and exits,
    # and this loop forks a replacement for the next connection.
    #
    # The master runs until it is asked to stop. `TERM`/`INT` stop it accepting, close the
    # listener, remove the socket file, and wait briefly for the children already serving --
    # the graceful half of what a service manager expects. A timeout on `accept` is what makes
    # the stop flag observable without a self-pipe.
    # `relay` may be nil, in which case `Volucer::Supervisor.relay` reads `VOLUCER_RELAY_SOCKET`.
    # That is the form a deployment should use: the server reads the same variable for its
    # `relay`, so the address is written once instead of in two files that nothing keeps in sync.
    # A value passed here still wins, so a script that names its own socket keeps meaning what it
    # says.
    def self.serve(relay, handler, children: nil)
      unless Process.respond_to?(:fork)
        raise NotImplementedError,
              'Volucer::Supervisor needs Process.fork, which this platform does not have; ' \
              'use a `worker` or `upstream` backend here, or run the supervisor on Unix'
      end

      unless children.nil? || children >= 1
        raise ArgumentError, "children must be nil (no cap) or at least 1, got #{children}"
      end

      # Resolved before the fork, so a missing address is one startup error rather than one per
      # child -- and so `listen` is never handed nil.
      address = self.relay(relay)
      if address.nil?
        raise ArgumentError,
              'no relay to listen on: pass one to Volucer::Supervisor.serve, or set ' \
              "#{RELAY_ENV} -- the same variable the server reads for its `relay`, " \
              'e.g. "unix:/run/app/workers.sock"'
      end

      limit = children

      listener = listen(address)

      stopping = false
      install_signals { stopping = true }

      announce(address, limit)

      live = {}
      begin
        until stopping
          reap(live)

          if !limit.nil? && live.length >= limit
            sleep SLOT_BACKOFF
            next
          end

          connection = accept_with_timeout(listener)
          next if connection.nil?

          child = fork do
            # --- the child ---
            # The master's bookkeeping is meaningless here and must not be inherited as if it
            # were this process's own children.
            live = nil
            listener.close
            serve_one(connection, handler)
          end

          # The *master* must close its copy. An inherited descriptor is not reference counted
          # by the protocol: leaving it open here means the child's end-of-file never happens,
          # so a recycled slot would hang instead of reconnecting.
          connection.close
          # Tracked rather than `Process.detach`ed: detaching reaps behind this loop's back, so
          # the live count would never fall and the supervisor would stop accepting after
          # `children` connections. `reap` is what collects them, from the top of the loop.
          live[child] = true
        end
      ensure
        listener.close
        path = relay_path(address)
        if path
          begin
            File.unlink(path)
          rescue Errno::ENOENT
            nil
          end
        end
      end
    end

    # One connection, served on a connection the supervisor opened.
    #
    # No wrapper is needed, unlike Python: a `TCPSocket` already responds to `read` and
    # `write`, which is what the SDK calls.
    #
    # `exit!` rather than `exit`, and that is deliberate twice over: normal shutdown would run
    # the master's `at_exit` handlers once per child, and it would flush buffers the master had
    # already written, so every child would write them again -- the classic duplicated-output
    # bug of a forked server.
    def self.serve_one(connection, handler)
      code = 0
      begin
        Worker.create(connection, connection).run(&handler)
      rescue Exception => e # rubocop:disable Lint/RescueException
        begin
          $stderr.write("[volucer] worker exited: #{e.class}: #{e.message}\n")
          $stderr.flush
        rescue StandardError
          nil
        end
        code = 1
      ensure
        begin
          connection.close
        rescue StandardError
          nil
        end
        exit!(code)
      end
    end

    # Accept one connection, returning nil when the timeout expires.
    #
    # `IO.select` rather than `accept_nonblock` in a retry loop: one call says whether a
    # connection is pending, and the timeout is the only reason this exists -- it is what lets
    # the loop notice its stop flag while no traffic is arriving.
    def self.accept_with_timeout(listener)
      ready = IO.select([listener], nil, nil, ACCEPT_TIMEOUT)
      return nil if ready.nil?

      listener.accept_nonblock(exception: false).then do |accepted|
        accepted.is_a?(Symbol) ? nil : accepted
      end
    end

    # Reap every child that has exited.
    #
    # `Process.waitpid(-1, WNOHANG)` in a loop is the shape that cannot block in the middle of
    # the accept loop. Nothing calls `Process.detach` here on purpose: a detached child is
    # reaped by a background thread, so the live count would never fall and the supervisor
    # would stop accepting after `children` connections. One reaper, one place.
    def self.reap(live)
      loop do
        pid = Process.waitpid(-1, Process::WNOHANG)
        break if pid.nil?

        live.delete(pid)
      end
    rescue Errno::ECHILD
      nil
    end

    # Install the shutdown handler, where the platform allows it.
    #
    # A library started from a non-main thread can still trap signals in Ruby, but the write
    # to a plain local from a trap handler is only safe because it is a boolean assignment;
    # a raise from a handler must not escape, so an unsupported signal is tolerated.
    def self.install_signals(&block)
      %w[TERM INT].each do |name|
        Signal.trap(name, &block)
      rescue ArgumentError, StandardError
        nil
      end
    end

    # The one readiness line a deployment needs.
    #
    # A unit file or a container healthcheck can wait for this line rather than guessing, and
    # it goes to stderr because stdout is the protocol channel.
    def self.announce(relay, children)
      cap = children.nil? ? 'no child cap' : "#{children} children at most"
      $stderr.write("[volucer] listening on #{relay} (#{cap})\n")
      $stderr.flush
    rescue StandardError
      nil
    end

    private_class_method :serve_one, :accept_with_timeout, :reap, :install_signals, :announce
  end
end
