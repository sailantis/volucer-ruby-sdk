# frozen_string_literal: true

require 'rake/testtask'

# The release tasks: `rake build`, `rake install` and `rake release`. The `release` task is
# what `rubygems/release-gem` runs, and it is not in the standard library — it comes from
# bundler, and it is what pushes the built gem to RubyGems.org.
#
# Required rather than optional, because a release that cannot be pushed is a release that
# has to be pushed by hand, and a hand-pushed gem is a gem whose provenance nobody checked.
require 'bundler/gem_tasks'

Rake::TestTask.new(:test) do |t|
  t.libs << 'lib' << 'test'
  t.test_files = FileList['test/test_*.rb']
  t.warning = true
end

task default: :test
