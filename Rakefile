task default: %w[test release]

task :test do
    Dir.glob("spec/*.rb").each do |test_case|
        ruby test_case
    end
end

task :release do
    `gem build gh-archive.gemspec`
    version = eval(File.read("gh-archive.gemspec")).version.to_s
    
    built_gem_filename = "gh-archive-#{version}.gem"
    if FileTest.exist?(built_gem_filename)
        `gem push "#{built_gem_filename}"`
    else
        warn "Unable to build the gem (expected existing file #{built_gem_filename})"
    end
end
