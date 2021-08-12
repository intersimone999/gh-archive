require 'code-assertions'
require 'json'
require 'open-uri'
require 'zlib'
require 'logger'
require 'tmpdir'
require 'thread/pool'
require 'thread/promise'

module GHAUtils
    def get_gha_filename(date)
        return ("%04d-%02d-%02d-%d.json.gz" % [date.year, date.month, date.day, date.hour])
    end
    
    def read_gha_file_content(gz)
        gzip = Zlib::GzipReader.new(gz)
        content = gzip.read
        gzip.close
        
        return content
    end
    
    def read_gha_file(gz)
        content = read_gha_file_content(gz)
            
        result = []
        content.lines.each do |line|
            result << JSON.parse(line)
        end
        
        return result
    end
    
    def each_date(from, to)
        current_date = from
        while current_date < to
            yield current_date
            current_date += 3600
        end
    end
end

class GHAProvider
    include GHAUtils
    
    def initialize
        @logger = Logger.new(STDOUT)
        
        @includes = {}
        @excludes = {}
    end
    
    def logger=(logger)
        @logger = logger
    end
    
    def get(date)
        raise "Not implemented"
    end
    
    def include(**args)
        args.each do |key, value|
            @includes[key.to_s] = [] unless @includes[key.to_s]
            @includes[key.to_s] << value
        end
    end
    
    def exclude(**args)
        args.each do |key, value|
            @excludes[key.to_s] = [] unless @excludes[key.to_s]
            @excludes[key.to_s] << value
        end
    end
    
    def each(from = Time.gm(2015, 1, 1), to = Time.now)
        self.each_date(from, to) do |current_date|
            events = []
            begin
                events = self.get(current_date)
                @logger.info("Scanned #{current_date}")
            rescue
                @logger.error($!)
                next
            end
            
            events.each do |event|
                skip = false
                @includes.each do |key, value|
                    skip = true unless value.include?(event[key])
                end
                
                @excludes.each do |key, value|
                    skip = true if value.include?(event[key])
                end
                next if skip
                
                yield event, current_date
            end
            
            events.clear
            GC.start
        end
    end
end

class OnlineGHAProvider < GHAProvider
    def initialize(max_retries = 3, proactive = false, proactive_pool_size = 10)
        super()
        
        @max_retries = max_retries
        @proactive = proactive
        @proactive_pool_size = proactive_pool_size
        @pool = Thread.pool(proactive_pool_size)
        @cache = Cache.new
    end
    
    def get(current_time)        
        @max_retries.times do
            begin
                filename = self.get_gha_filename(current_time)
                
                if @proactive
                    @logger.info("Waiting for cache to have #{current_time}...") unless @cache.has?(filename)
                    
                    while !@cache.has?(filename)
                        sleep 1
                    end

                    return @cache.get(filename)
                else
                    URI.open("http://data.gharchive.org/#{filename}") do |gz|
                        return self.read_gha_file(gz)
                    end
                end
            rescue Errno::ECONNRESET
                next
            rescue Zlib::GzipFile::Error
                raise $!
            rescue
                @logger.warn($!)
            end
        end
        
        raise DownloadArchiveException, "Exceeded maximum number of tentative downloads."
    end
    
    def cache(current_time)
        @logger.info("Full cache. Waiting for some free slot...") if @cache.full?
        while @cache.full?
            sleep 1
        end
        @max_retries.times do
            begin
                filename = self.get_gha_filename(current_time)
                URI.open("http://data.gharchive.org/#{filename}") do |gz|
                    content = self.read_gha_file(gz)
                    @cache.put(filename, content)
                    return
                end
            rescue Errno::ECONNRESET
                next
            rescue Zlib::GzipFile::Error
                raise $!
            rescue
                @logger.warn($!)
            end
        end
    end
    
    def each(from = Time.gm(2015, 1, 1), to = Time.now)
        if @proactive
            any_ready = Thread.promise
            
            @logger.info("Proactively scheduling download tasks...")
            self.each_date(from, to) do |current_date|
                @pool.process(current_date) do |current_date|
                    cache(current_date)
                    any_ready << true
                    @logger.info("Proactively cached #{current_date}. Cache size: #{@cache.size}")
                end
            end
            
            ~any_ready
            @logger.info("Download tasks successfully scheduled!")
        end
        
        super
    end
    
    class Cache
        def initialize(max_size = 10)
            @cache = {}
            @max_size = max_size
            @mutex = Mutex.new
        end
        
        def put(name, content)
            @mutex.synchronize do
                @cache[name] = content
            end
        end
        
        def get(name)
            @mutex.synchronize do
                return @cache.delete(name)
            end
        end
        
        def size
            @mutex.synchronize do
                return @cache.size
            end
        end
        
        def has?(name)
            return @cache.has_key?(name)
        end
        
        def full?
            self.size >= @max_size
        end
    end
    
    class DownloadArchiveException < Exception
    end
end

class FolderGHAProvider < GHAProvider
    def initialize(folder)
        super()
        
        @folder = folder
    end
    
    def get(current_time)        
        filename = self.get_gha_filename(current_time)
        File.open(File.join(@folder, filename), "rb") do |gz|
            return self.read_gha_file(gz)
        end
    end
end

class GHADownloader
    include GHAUtils
    
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
        self.each_date(from, to) do |current_date|
            filename = self.get_gha_filename(current_date)
            out_filename = filename.clone
            out_filename.gsub!(".json.gz", ".json") if @decompress
            
            target_file = File.join(@folder, out_filename)
            if FileTest.exist?(target_file)
                @logger.info("Skipping existing file for #{current_date}")
                next
            else
                @logger.info("Downloading file for #{current_date}")
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
