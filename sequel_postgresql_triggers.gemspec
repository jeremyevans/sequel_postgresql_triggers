spec = Gem::Specification.new do |s|
  s.name = "sequel_postgresql_triggers"
  s.version = "1.5.0"
  s.author = "Jeremy Evans"
  s.email = "code@jeremyevans.net"
  s.platform = Gem::Platform::RUBY
  s.summary = "Database enforced timestamps, immutable columns, counter/sum caches, and touch propogation"
  s.files = %w'README.rdoc MIT-LICENSE lib/sequel_postgresql_triggers.rb lib/sequel/extensions/pg_triggers.rb spec/sequel_postgresql_triggers_spec.rb'
  s.license = 'MIT'
  s.homepage = 'https://github.com/jeremyevans/sequel_postgresql_triggers' 
  s.rdoc_options = ['--inline-source', '--line-numbers', '--title', 'Sequel PostgreSQL Triggers: Database enforced timestamps, immutable columns, and counter/sum caches', 'README.rdoc', 'MIT-LICENSE', 'lib']
  s.required_ruby_version = ">= 1.9.2"
  s.add_dependency('sequel')
end
