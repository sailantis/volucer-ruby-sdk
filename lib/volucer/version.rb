# frozen_string_literal: true

module Volucer
  # The gem version, in a file of its own.
  #
  # **The gemspec must not require `volucer` itself.** That file rewrites `$stdout` so an
  # application's `puts` cannot corrupt the worker channel, and a gemspec is evaluated by
  # bundler on *every* `bundle` command — so requiring the library from the gemspec gave
  # ordinary commands like `bundle exec rake test` a hijacked stdout. On Ruby 4, rake calls
  # `$stdout.set_encoding`, `GuardedStdout` does not implement it, and the release died with
  # `undefined method 'set_encoding' for an instance of Volucer::GuardedStdout`.
  #
  # Reading one constant is all a gemspec should ever need from its own library.
  VERSION = '0.1.0'
end