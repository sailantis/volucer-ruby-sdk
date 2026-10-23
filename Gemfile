# frozen_string_literal: true

source 'https://rubygems.org'

# The gem itself, so `rake release` builds and pushes the checkout rather than an
# installed copy of it. The release workflow's `bundle exec rake release` needs
# this file to exist: bundler is what resolves the gem's own path.
gemspec

# The release tasks come from `bundler/gem_tasks`, which the Rakefile requires.
# This is the version that provides them, and the floor is the one that has the
# `release` task's tag-and-push behaviour the release action depends on.
gem 'bundler', '>= 2.4'
