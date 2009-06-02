spec = Gem::Specification.new do |s|
  s.name = "sequel_postgresql_triggers"
  s.version = "1.0.2"
  s.author = "Jeremy Evans"
  s.email = "code@jeremyevans.net"
  s.platform = Gem::Platform::RUBY
  s.summary = "Database enforced timestamps, immutable columns, and counter/sum caches"
  s.files = %w'README LICENSE lib/sequel_postgresql_triggers.rb spec/sequel_postgresql_triggers_spec.rb'
  s.require_paths = ["lib"]
  s.has_rdoc = true
  s.rdoc_options = ['--inline-source', '--line-numbers', '--title', 'Sequel PostgreSQL Triggers: Database enforced timestamps, immutable columns, and counter/sum caches', 'README', 'LICENSE', 'lib']
end
