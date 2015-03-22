require "rake"
require "rake/clean"

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

begin
  begin
    raise LoadError if ENV['RSPEC1']
    # RSpec 2
    require "rspec/core/rake_task"
    spec_class = RSpec::Core::RakeTask
    spec_files_meth = :pattern=
  rescue LoadError
    # RSpec 1
    require "spec/rake/spectask"
    spec_class = Spec::Rake::SpecTask
    spec_files_meth = :spec_files=
  end

  desc "Run specs"
  spec_class.new("spec") do |t|
    t.send(spec_files_meth, ["./spec/*_spec.rb"])
  end
  task :default=>[:spec]
rescue LoadError
  task :default do
    puts "Must install rspec to run the default task (which runs specs)"
  end
end

desc "Package sequel_postgresql_triggers"
task :package do
  sh %{gem build sequel_postgresql_triggers.gemspec}
end
