require_relative 'core'

module GHArchive
    class Downloader
        include Utils
        
        def initialize(folder, decompress = false)
            @logger = Logger.new(STDERR)
            @decompress = decompress
            @folder = folder
            @max = nil
            
            Dir.mkdir(@folder) unless FileTest.exist?(@folder)
            raise "A file exist with the desired folder name #{folder}" unless FileTest.directory?(@folder)
        end
        
        def max(max)
            @max = max
            return self
        end
        
        def logger=(logger)
            @logger = logger
        end
        
        def download(from = Time.gm(2015, 1, 1), to = Time.now)
            archive = []
            self.each_time(from, to) do |current_time|
                filename = self.get_gha_filename(current_time)
                out_filename = filename.clone
                out_filename.gsub!(".json.gz", ".json") if @decompress
                
                target_file = File.join(@folder, out_filename)
                if FileTest.exist?(target_file)
                    @logger.info("Skipping existing file for #{current_time}")
                    next
                else
                    @logger.info("Downloading file for #{current_time}")
                end
                
                File.open(target_file, 'w') do |f|
                    URI.open("http://data.gharchive.org/#{filename}") do |gz|
                        if @decompress
                            f << self.read_gha_file_content(gz)
                        else
                            f << gz.read
                        end
                    end
                end
                archive << target_file
                
                if @max && archive.size > @max
                    last = archive.shift
                    @logger.info("Removing local file #{last}")
                    File.unlink(last)
                end
                
                yield filename if block_given?
            end
        end
    end
end
