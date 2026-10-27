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

# `bundler` is deliberately **not** listed. `bundler/gem_tasks` -- which provides the
# `release` task -- is supplied by whichever bundler is running, so a Gemfile entry only
# imposes a floor on every Ruby that evaluates this file. Asking for `>= 2.4` broke
# Ruby 3.1, whose bundled bundler is 2.3.27: "Your bundle requires a different version of
# Bundler than the one you're running". That failure was invisible while CI ran a bare
# `rake test` and only appeared once it ran `bundle exec`.

# The suite's test framework. It is a bundled gem on modern Rubies rather than a default one,
# so it resolves when the tests are run directly but not under `bundle exec`.
#
# Pinned below 6, which raised its floor to Ruby 3.2: CI still covers 3.1, and an
# unconstrained `gem 'minitest'` resolves 6.x there and then fails to install.
gem 'minitest', '< 6'

# Only needed on Windows, where `Volucer.process_rss_bytes` uses `GetProcessMemoryInfo`.
# It stopped being a default gem in Ruby 4.0, so without this line the RSS path degrades to 0
# under bundler — the suite still passes, which is exactly why it is listed here: the Gemfile
# should exercise the code that actually runs in a deployment, not its fallback.
gem 'fiddle', platforms: :windows