# frozen_string_literal: true

source 'https://rubygems.org'

# The gem itself, so `rake release` builds and pushes the checkout rather than an
# installed copy of it. The release workflow's `bundle exec rake release` needs
# this file to exist: bundler is what resolves the gem's own path.
gemspec

# **`rake` must be listed, not assumed.** Nothing in a `.gemspec` puts it in the bundle, and
# `rubygems/release-gem` runs `bundle exec rake release`. On Ruby 3.x the default `rake` gem
# happened to be reachable anyway, so a bare `rake test` worked; on Ruby 4 it is not, and the
# release failed with "can't find executable rake for gem rake. rake is not currently
# included in the bundle".
gem 'rake'

# The release tasks come from `bundler/gem_tasks`, which the Rakefile requires.
# This is the version that provides them, and the floor is the one that has the
# `release` task's tag-and-push behaviour the release action depends on.
gem 'bundler', '>= 2.4'

# The suite's test framework. It is a bundled gem on modern Rubies rather than a default one,
# so it resolves when the tests are run directly but not under `bundle exec`.
gem 'minitest'

# Only needed on Windows, where `Volucer.process_rss_bytes` uses `GetProcessMemoryInfo`.
# It stopped being a default gem in Ruby 4.0, so without this line the RSS path degrades to 0
# under bundler — the suite still passes, which is exactly why it is listed here: the Gemfile
# should exercise the code that actually runs in a deployment, not its fallback.
gem 'fiddle', platforms: :windows