require "rake"
require "rake/clean"
require "spec/rake/spectask"

CLEAN.include ["*.gem", "rdoc"]
RDOC_OPTS = ["--quiet", "--line-numbers", "--inline-source", '--title', \
  'Sequel PostgreSQL Triggers: Database enforced timestamps, immutable columns, and counter/sum caches', '--main', 'README']

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
  rdoc.rdoc_files.add %w"README LICENSE lib/sequel_postgresql_triggers.rb"
end

desc "Run specs"
Spec::Rake::SpecTask.new("spec") do |t|
  t.spec_files = ["spec/sequel_postgresql_triggers_spec.rb"]
end
task :default=>[:spec]

desc "Package sequel_postgresql_triggers"
task :package do
  sh %{gem build sequel_postgresql_triggers.gemspec}
end
