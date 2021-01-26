Gem::Specification.new do |s|
  s.name        = 'gh-archive'
  s.version     = '0.1'
  s.date        = '2021-01-26'
  s.summary     = "GitHub Archive mining utility"
  s.description = "Download and analyze the GitHub events stored at GitHub archive"
  s.authors     = ["Simone Scalabrino"]
  s.email       = 's.scalabrino9@gmail.com'
  s.files       = Dir.glob("lib/*.rb")
  s.homepage    = 'https://github.com/intersimone999/gh-archive'
  s.license     = "GPL-3.0-only"
  
  s.add_runtime_dependency "code-assertions", "~> 1.1.2", ">= 1.1.2"
end
