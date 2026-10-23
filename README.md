# volucer-ruby-sdk

A Ruby worker for [volucer](https://github.com/sailantis/volucer)'s worker protocol.

The specification is [`docs/worker-protocol.md`](https://github.com/sailantis/volucer/blob/main/docs/worker-protocol.md)
in the server repository. This gem is the other end of it, beside the PHP one in
[`sailantis/volucer-php-sdk`](https://github.com/sailantis/volucer-php-sdk) and the Python
one in [`sailantis/volucer-python-sdk`](https://github.com/sailantis/volucer-python-sdk). All
four implement the same document rather than each other, and the wire-format tests in each
assert the same frozen bytes.

## What a worker is

Worker mode boots an application once and serves many requests from that process, instead of
booting it per request. It is the RoadRunner/Swoole/FrankenPHP value proposition, and it is
why a real framework's bootstrap cost stops being charged on every request — a Rails boot is
milliseconds, and on a small route it is most of the request.

A Ruby application already has servers that do this — Puma, Falcon, Thin. The reason to use
this SDK instead is the channel rather than the model: it runs over the child's stdin/stdout
(or a loopback socket on Windows), so the application keeps its own process, its own
container and its own warm caches, and volucer supervises it without wrapping it in a
protocol library first.

**If you want the shortest path to "my Rails or Sinatra app runs behind volucer", use the
`upstream` backend instead.** It supervises Puma over HTTP, needs no gem, and is what the
plan recommends first. This gem exists for the case where the worker channel itself is what
you want: the `kv`, `lock`, `jobs` and `metrics` services, recycling on RSS, and no HTTP
framing on the hot path.

## Install

```console
gem install volucer-worker
```

Or, in a Gemfile:

```ruby
gem 'volucer-worker'
```

No runtime dependencies beyond the standard library, deliberately: a worker SDK is loaded
before an application's own gems are known to work, and a dependency here is a failure mode
unrelated to the protocol.

## A worker, in three lines

Any Rack application works, including a Rails app:

```ruby
require 'volucer'
require 'my_app'

Volucer::Worker.connect.run(&Volucer::Rack.handler(MyApp.new))
```

`Volucer::Worker.connect` honours `VOLUCER_WORKER_SOCKET` and falls back to stdio. It is the
call a worker file should make; `Volucer::Worker.create` is the explicit-streams form, used
when you are binding streams yourself — including when the connection was opened by somebody
else, which is what the `attach` backend needs.

The variable carries either form, and `connect` decides from its prefix:

| value | transport |
| --- | --- |
| `127.0.0.1:34567` | loopback TCP — the ephemeral port the server bound |
| `unix:/run/volucer/worker.0.sock` | a unix socket, from `socket = "unix:/run/volucer/worker"` |

`Volucer.unix_path` is the whole of the decision and returns `nil` for a TCP address, so the
connection code has one branch. `unix:` is the same convention the server's configuration uses for
`external`, `attach` and the worker `socket` key, so there is one spelling rather than a fourth.

## Attaching instead of spawning

The `worker` backend starts your process. The `attach` backend does not: it *dials* a relay
that a supervisor in your own process listens on, and holds one connection per slot. That is
the shape that lets a supervisor fork its workers after booting the application once, so every
child inherits the interpreter, the loaded classes and Rails' initialisation instead of paying
for them again.

The split matters: **volucer cannot own the fork.** It has to happen in the application's
process, because the side that owns the listening socket has to be the side that owns the fork
— and because `fork` in a process that already has threads is not something a library can
decide safely. So the fork lives in `Volucer::Supervisor`, and you call it:

```ruby
# supervisor.rb — boot once, then fork
require "volucer"
require_relative "config/environment"     # Rails' own boot

# Boot before the fork: whatever this closes over is inherited by every child.
handler = Volucer::Rack.handler(Rails.application)

Volucer::Supervisor.serve(nil, handler)      # or "unix:/run/app/workers.sock"
```

```toml
[backends.rails]
kind    = "attach"
workers = 16
```

**The address can be written once.** With `nil` as the first argument the supervisor reads
`VOLUCER_RELAY_SOCKET`, and so does the server when `relay` is omitted:

```ini
# the unit file, or a container's environment
Environment=VOLUCER_RELAY_SOCKET=unix:/run/app/workers.sock
```

That is the counterpart of `VOLUCER_WORKER_SOCKET`, for the mirror-image reason: that one is set
*by* the server on a child it spawned, so a supervisor cannot read it — no child was spawned. This
one is set by the operator, and both sides read it. A value passed to `serve`, or a `relay` key in
the file, still wins, so a stale variable cannot move a deployment. `Supervisor.relay` is that
resolution on its own.

`children` caps how many workers may run at once, and omitting it means **no cap**, which is
what an `attach` deployment wants: volucer dials `workers` times at startup and holds one
connection per slot, so the live children are already bounded by that number. A cap **below**
`workers` is not a queue — the surplus dials sit unaccepted in the listen backlog while the pool
waits for a handshake no child is there to send.

Each child serves exactly one connection and exits. That is what makes recycling cheap — when
`max_requests` is reached volucer closes the slot, the child sees end-of-file, exits, and the
supervisor forks a replacement for the next connection. The supervisor reaps its children, so
a long-lived one does not fill the process table with zombies.

**Unix only.** `Process.fork` does not exist on Windows, and `serve` raises there rather than
silently running a single process. Use `kind = "worker"` or `kind = "upstream"` on Windows.

**Boot before the fork, connect after it.** The application and its loaded classes are
inherited by every child, which is the saving. An *open connection* must not be: every child
would inherit the socket and they would take turns reading from it. Rails is safe here because
ActiveRecord checks connections out lazily, per request, and `ActionDispatch::Executor` returns
them to the pool afterwards — which is why `Volucer::Rack.handler` is the right wrapper for a
Rails app and a bare `proc` is not.

### Writing a supervisor yourself

`Volucer::Supervisor.serve` is a convenience, not the contract. What `attach` needs from any
supervisor is that it serves a connection **it** opened, which is `Worker.create`:

```ruby
require "socket"

listener = TCPServer.new("127.0.0.1", 9700)

loop do
  connection = listener.accept
  child = fork do
    listener.close
    Volucer::Worker.create(connection, connection).run(&handler)
    exit!(0)
  end
  connection.close    # the master must drop its copy
end
```

No wrapper is needed here, unlike Python: a `TCPSocket` already responds to `read` and `write`,
which is what the worker calls on its streams. `exit!` rather than `exit` skips the master's
`at_exit` handlers and buffer flushing, which every child would otherwise run again.

## Rails

Rails' `Application` object is already a Rack app, so there is nothing to adapt:

```ruby
# worker.rb
require "volucer"
require_relative "config/environment"   # Rails' own boot

Volucer::Worker.connect.run(&Volucer::Rack.handler(Rails.application))
```

```toml
[backends.rails]
kind    = "worker"
command = "ruby worker.rb"
workers = 4

[server]
script_extensions = ["rb"]   # else /worker.rb is PATH_INFO, not a routed script
```

That is the whole integration. **You do not need `Rails.application.executor.wrap`**:
`ActionDispatch::Executor` is in `Rails::Application`'s default middleware stack, so
`Rails.application.call(env)` is already wrapped and ActiveRecord connections return to the
pool after each request. In development `Rails.application.reloader` is likewise already in
the stack, so code reload keeps working.

Verified against Rails 8.1.4 and a real `volucer` binary: routing, `request.original_url`,
`request.host`, sessions in both directions, Active Record across consecutive requests, the
pool's checked-out count returning to zero, and a 404 staying a 404.

## Sinatra and other Rack apps

Identical, with your app in place of `Rails.application`. A bare Rack app that holds a
connection pool should wrap `call` itself with its own executor — that is the one case where
the wrapping is yours rather than the framework's.

Point a backend at it:

```toml
[backends.ruby]
kind = "worker"
command = "ruby worker.rb"
workers = 4
memory_limit_mb = 256
script_extensions = ["rb"]
```

Two configuration keys are not optional for a non-PHP worker:

- `script_extensions` must name the extension your entry point has, or `/app.rb` arrives at
  the front controller as `PATH_INFO` instead of being routed. A list naming `rb` *replaces*
  the default rather than adding to it, so a site serving Ruby stops treating `.php` as a
  script — which is what "this tree is Ruby" means.
- `workers` is the concurrency, because the protocol carries **one request in flight per
  worker process**. Puma with threads is what a Ruby deployment normally uses; this channel
  is the opposite trade, and it is deliberate rather than a gap.

## The two Ruby-specific answers

Everything else in this gem is a transliteration of the specification. These two are not.

### stdout is the channel, so the SDK takes it

The protocol uses the child's stdout as its channel. PHP solves this with an output buffer
around `echo`, the Python SDK rebinds `sys.stdout`, and Ruby's answer is that **requiring
`volucer` reassigns `$stdout`** to a stream that writes to `$stderr`, keeping the real channel
in `Volucer::CHANNEL_OUT`.

The consequence is worth stating plainly: `puts` still reaches a human (through stderr, which
the supervisor drains), but it will not appear on the process's own stdout. A worker that did
not do this would corrupt its framing the first time somebody left a `puts` in, and the
symptom would be a killed worker rather than a stray line.

`Rails.logger` goes wherever it is configured to go, and the Rails default in a worker
process is not stdout — but a `puts` in a controller is exactly the case this protects
against.

### RSS has no exact per-request answer in Ruby

The server recycles a worker whose reported `peak_memory_bytes` exceeds `memory_limit_mb`,
and the protocol wants that figure **per request, not per process**: a high-water mark that
never falls would turn a leak detector into a per-request application boot.

PHP has `memory_get_peak_usage()` plus `memory_reset_peak_usage()`, which is the right shape.
Ruby does not: `GC.stat` reports heap slots rather than resident bytes, and neither it nor
`ObjectSpace` answers "how much memory did that request cost". So this gem reports the
process's **resident set size** — `K32GetProcessMemoryInfo` on Windows, `/proc/self/statm`
elsewhere — and that number is cumulative by nature.

| Reported | Behaviour |
| --- | --- |
| The process RSS | Cheap, and **cannot fall**, so a peak that once exceeded `memory_limit_mb` recycles the worker after every request |

The practical advice is the same as for Python: report what you can, and treat
`memory_limit_mb` as a ceiling rather than a per-request budget. A worker that reports `0`
("this runtime does not report it") is also accepted by the server, and one that does not
report is never recycled on memory. Override `Worker#peak_memory_bytes` — it is a private
method on a class you control — if you want to feed it `jemalloc`'s numbers or a
`GC.stat(:heap_allocated_pages)`-derived estimate instead.

## The parts of the protocol a handler sees

| Frame | Handler surface |
| --- | --- |
| `REQUEST` / `RESPONSE` / `BODY` / `END` | `Worker#run { }`; the block returns a `Response`, a `String`, a Rack `[status, headers, body]` array, or `nil` |
| `JOB` / `ACK` | `Worker#consume { }`; `nil` acknowledges, `false` requeues, a `String` rejects with a reason, and a raise requeues |
| `CALL` / `RETURN` | `Worker#call(name, params)` from inside a block; `Worker#on_call { }` answers the server's calls |
| `LOG` / `METRIC` | `Worker#emit_log`, `Worker#emit_metric` |
| `CONTROL` | Answered by the SDK; `drain` and `shutdown` stop the loop |
| `PING` / `PONG` | Answered by the SDK |

`Worker#recycle` asks to be replaced after the current request, reported as
`keep_alive = false` in `END`.

## gRPC

A gRPC call arrives as an ordinary `REQUEST` whose `content-type` is `application/grpc…`, and
its outcome travels in the reply's trailer section rather than in the HTTP status. The gem
exposes that as `Volucer::GrpcResponse`:

```ruby
worker = Volucer::Worker.connect
worker.on_grpc { |request, _worker| Volucer::GrpcResponse.message(encode_reply(request)) }
worker.run { |request, _worker| handle(request) }
```

`GrpcResponse.message(payload)` takes the **framed bytes** — the compression flag and the
big-endian length included. This gem does not ship a protobuf encoder, because doing so would
put a schema in the transport.

## Tests

```console
rake test
```

The wire-format tests assert the same frozen bytes as
`crates/volucer-worker/tests/wire_format.rs`, `volucer-php-sdk/tests/WireFormatTest.php` and
`volucer-python-sdk/tests/test_wire_format.py`, so all four cannot drift apart silently.

The end-to-end test drives a real `volucer` binary and is skipped unless one is found:

```console
VOLUCER_BIN=/path/to/volucer rake test
```

It looks for `../volucer/target/debug/volucer` when `VOLUCER_BIN` is unset, so a developer
working in the sibling checkout needs no environment at all. `VOLUCER_E2E=1` turns a skip
into a failure, which is what CI sets — a skipped end-to-end test and a passing one are the
same green.

## What this gem does not do

- **Falcon/async or thread-per-request concurrency inside one worker.** The channel carries
  one request at a time, so that is a protocol change rather than an adapter.
- **Streaming responses.** `RESPONSE` declares `body_len` before the body, so a response is
  complete before it starts. A protocol property, not a missing feature here.
- **Per-request state reset for your framework.** The Rack environment is rebuilt by the gem,
  so that much is reset; a framework that keeps module-level state or a cached connection has
  to deal with it itself. **Rails does not need anything extra**: `ActionDispatch::Executor` is
  in `Rails::Application`'s default middleware stack, so `Rails.application.call(env)` is
  already wrapped and ActiveRecord connections go back to the pool on their own — verified by
  driving two requests through a real `volucer` binary and seeing the pool's checked-out count
  return to zero. `Rails.application.executor.wrap` is the answer for a **bare Rack app** that
  shares a connection pool, or for a caller driving the app outside Rack, not for this gem's
  Rails path. The check that matters is two consecutive requests that must not see each other's
  state, and `test/test_end_to_end.rb` asserts exactly that for the gem's own surface.
- **Forking on Windows.** `Volucer::Supervisor.serve` needs `Process.fork`. The platform without
  it is served by `kind = "worker"` or `kind = "upstream"`, and the end-to-end suite proves the
  `attach` contract on Windows with a threaded supervisor instead.
