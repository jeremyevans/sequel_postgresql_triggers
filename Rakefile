require "rake"
require "rake/clean"

CLEAN.include ["*.gem", "rdoc"]
RDOC_OPTS = ["--quiet", "--line-numbers", "--inline-source", '--title', \
  'Sequel PostgreSQL Triggers: Database enforced timestamps, immutable columns, and counter/sum caches', '--main', 'README.rdoc']

rdoc_task_class = begin
  require "rdoc/task"
  RDOC_OPTS.concat(['-f', 'hanna'])
  RDoc::Task
rescue LoadError
  require "rake/rdoctask"
  Rake::RDocTask
end

rdoc_task_class.new do |rdoc|
  rdoc.rdoc_dir = "rdoc"
  rdoc.options += RDOC_OPTS
  rdoc.rdoc_files.add %w"README.rdoc MIT-LICENSE lib/sequel_postgresql_triggers.rb lib/sequel/extensions/pg_triggers.rb"
end

desc "Run specs with extension"
task :spec do
  sh "#{FileUtils::RUBY} -I lib spec/sequel_postgresql_triggers_spec.rb"
end

desc "Run specs with global modification"
task :spec_global do
  begin
    ENV['PGT_GLOBAL'] = '1'
    sh "#{FileUtils::RUBY} -I lib spec/sequel_postgresql_triggers_spec.rb"
  ensure
    ENV.delete('PGT_GLOBAL')
  end
end

desc "Run all specs"
task :default => [:spec, :spec_global]

desc "Package sequel_postgresql_triggers"
task :package do
  sh %{gem build sequel_postgresql_triggers.gemspec}
end
