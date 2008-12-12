require "rake"
require "rake/clean"
require "spec/rake/spectask"
begin
  require "hanna/rdoctask"
rescue LoadError
  require "rake/rdoctask"
end

CLEAN.include ["*.gem", "rdoc"]
RDOC_OPTS = ["--quiet", "--line-numbers", "--inline-source", '--title', \
  'Sequel PostgreSQL Triggers: Database enforced timestamps, immutable columns, and counter/sum caches', '--main', 'README']

Rake::RDocTask.new do |rdoc|
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
