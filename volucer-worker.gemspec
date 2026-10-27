# frozen_string_literal: true

# Only the version, deliberately: requiring `lib/volucer` here would rewrite `$stdout` at
# gemspec-evaluation time, which bundler does on every `bundle` command. See
# `lib/volucer/version.rb` for what that broke.
require_relative 'lib/volucer/version'

Gem::Specification.new do |spec|
  spec.name = 'volucer-worker'
  spec.version = Volucer::VERSION
  spec.authors = ['Sailantis']
  spec.summary = 'A Ruby worker SDK for the volucer worker protocol'
  spec.description = <<~DESC
    The Ruby end of volucer's worker protocol. The specification is
    `docs/worker-protocol.md` in sailantis/volucer; this gem implements it beside the PHP
    and Python SDKs. Includes a Rack adapter, CALL/RETURN and JOB support, an `[rpc]` client
    for applications that are not workers, and no runtime dependencies beyond the standard
    library.
  DESC
  spec.homepage = 'https://github.com/sailantis/volucer-ruby-sdk'
  spec.license = 'MIT'
  spec.required_ruby_version = '>= 3.1'

  spec.files = Dir['lib/**/*.rb'] + ['README.md', 'LICENSE']
  spec.require_paths = ['lib']

  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = spec.homepage
  spec.metadata['documentation_uri'] =
    'https://github.com/sailantis/volucer/blob/main/docs/worker-protocol.md'
end
