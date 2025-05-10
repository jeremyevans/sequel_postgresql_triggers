require "rake"
require "rake/clean"

CLEAN.include ["*.gem", "rdoc"]

desc "Generate rdoc"
task :rdoc do
  rdoc_dir = "rdoc"
  rdoc_opts = ["--line-numbers", "--inline-source", '--title', 'Sequel PostgreSQL Triggers: Database enforced timestamps, immutable columns, and counter/sum caches']

  begin
    gem 'hanna'
    rdoc_opts.concat(['-f', 'hanna'])
  rescue Gem::LoadError
  end

  rdoc_opts.concat(['--main', 'README.rdoc', "-o", rdoc_dir] +
    %w"README.rdoc CHANGELOG MIT-LICENSE" +
    Dir["lib/**/*.rb"]
  )

  FileUtils.rm_rf(rdoc_dir)

  require "rdoc"
  RDoc::RDoc.new.document(rdoc_opts)
end

test_flags = String.new
test_flags << " -w" if RUBY_VERSION >= '3'
test_flags << " -W:strict_unused_block" if RUBY_VERSION >= '3.4'

desc "Run specs with extension"
task :spec do
  sh "#{FileUtils::RUBY} #{test_flags} spec/sequel_postgresql_triggers_spec.rb"
end

desc "Run specs with global modification"
task :spec_global do
  begin
    ENV['PGT_GLOBAL'] = '1'
    sh "#{FileUtils::RUBY} #{test_flags} spec/sequel_postgresql_triggers_spec.rb"
  ensure
    ENV.delete('PGT_GLOBAL')
  end
end

desc "Run all specs"
task :default => [:spec, :spec_global]

desc "Run specs with coverage"
task :spec_cov do
  ENV["COVERAGE"] = "extension"
  Rake::Task['spec'].invoke
  ENV["COVERAGE"] = "global"
  Rake::Task['spec_global'].invoke
  ENV.delete('COVERAGE')
end

desc "Run specs in CI"
task :spec_ci do
  ENV['PGT_SPEC_DB'] = "#{RUBY_ENGINE == 'jruby' ? 'jdbc:postgresql' : 'postgres'}://localhost/?user=postgres&password=postgres"
  Rake::Task['default'].invoke
end

desc "Package sequel_postgresql_triggers"
task :package do
  sh %{gem build sequel_postgresql_triggers.gemspec}
end
