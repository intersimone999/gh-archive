require 'code-assertions'
require 'json'
require 'open-uri'
require 'zlib'
require 'logger'
require 'tmpdir'
require 'thread/pool'
require 'thread/promise'

require_relative File.expand_path('../gh-archive/events', __FILE__)

module GHArchive
    class ThreadPool
        def initialize(size)
            @size = size
            @threads = []
            @queue = []
            @mutex = Mutex.new
            
            @consumer_thread = Thread.start do
                while !@shutdown || @threads.size > 0 || @queue.size > 0
                    sleep 0.1 if @queue.size == 0 || @threads.size == @size
                    @threads.delete_if { |t| !t.alive? }
                    
                    if @threads.size < @size && @queue.size > 0
                        @mutex.synchronize do
                            args, job = @queue.shift
                            @threads << Thread.start(*args, &job)
                        end
                    end
                end
            end
        end
        
        def process(*args, &block)
            raise "Block expected" unless block_given?
            raise "Can not add jobs while shutting down" if @shutdown
            
            @mutex.synchronize do
                @queue << [args, block]
            end
            
            return self.enqueued
        end
        
        def shutdown
            @shutdown = true
        end
        
        def shutdown!
            self.shutdown
            @mutex.synchronize do
                @queue.clear
            end
        end
        
        def enqueued
            return @queue.size
        end
        
        def shutdown?
            @shutdown
        end
        
        def alive?
            @consumer_thread.alive?
        end
        
        def wait
            while alive?
                sleep 0.1
            end
        end
    end
end

module GHAUtils
    def get_gha_filename(date)
        return ("%04d-%02d-%02d-%d.json.gz" % [date.year, date.month, date.day, date.hour])
    end
    
    def read_gha_file_content(gz)
        gzip = Zlib::GzipReader.new(gz)
        return gzip.read
    ensure
        gzip.close if gzip
    end
    
    def read_gha_file(file)
        
        if !file.is_a?(StringIO) && file.path.end_with?(".json")
            content = file.read
        elsif file.is_a?(StringIO) || file.path.end_with?(".gz") || file.path.start_with?("/tmp/open-uri")
            content = read_gha_file_content(file)
        else
            raise "Invalid file extension for #{file.path}: expected `.json.gz` or `json`,"
        end
            
        result = []
        content.lines.each do |line|
            result << JSON.parse(line)
        end
        
        return result
    end
    
    def each_time(from, to)
        current_time = from
        while current_time < to
            yield current_time
            current_time += 3600
        end
    end
end

class GHAProvider
    include GHAUtils
    
    def initialize
        @logger = Logger.new(STDOUT)
        
        @includes = {}
        @excludes = {}
        
        @checkpoint_name = nil
        @use_json = true
    end
    
    def use_checkpoint(filename)
        @checkpoint_name = filename
        
        return self
    end
    
    def parse_events
        @use_json = false
        
        return self
    end
    
    def logger=(logger)
        @logger = logger
        
        return self
    end
    alias :use_logger :logger=
    
    def get(date)
        raise "Not implemented"
    end
    
    def include(**args)
        args.each do |key, value|
            @includes[key.to_s] = [] unless @includes[key.to_s]
            @includes[key.to_s] << value
        end
        
        return self
    end
    
    def exclude(**args)
        args.each do |key, value|
            @excludes[key.to_s] = [] unless @excludes[key.to_s]
            @excludes[key.to_s] << value
        end
        
        return self
    end
    
    def restore_checkpoint(from)
        if @checkpoint_name && FileTest.exist?(@checkpoint_name)
            # Note that this throws an exception if the file is not readable. This is the intended behavior.
            # As opposed to that, failing to save the checkpoint information just results in a warning on the log.
            loaded_from = Marshal.load(File.read(@checkpoint_name))
            raise "The loaded checkpoint (#{loaded_from}) occurs before the current from date (#{from})" if loaded_from < from
            
            @logger.info("Valid checkpoint loaded. Restored execution from #{loaded_from}.")
            
            return loaded_from
        else
            return from
        end
    end
    
    def update_checkpoint(current_time)
        if @checkpoint_name
            begin
                File.open(@checkpoint_name, "wb") do |f|
                    f.write(Marshal.dump(current_time))
                end
            rescue
                @logger.warn(
                    "Unable to save the checkpoint at the specified location (#{File.expand_path(@checkpoint_name)})."
                )
            end
        end
    end
    
    def each(from = Time.gm(2015, 1, 1), to = Time.now)
        exceptions = []
        
        from = restore_checkpoint(from)
        
        self.each_time(from, to) do |current_time|
            events = []
            
            update_checkpoint(current_time)
            
            begin
                events = self.get(current_time)
            rescue GHAException => e
                @logger.warn(e.message)
                next
            rescue => e
                @logger.error("An exception occurred for #{current_time}: #{e.message}")
                exceptions << e
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
                
                if @use_json
                    yield event, current_time
                else
                    yield GHArchive::Event.parse(event), current_time
                end
            end
            
            @logger.info("Scanned #{current_time}")
            
            events.clear
            GC.start
        end
        
        update_checkpoint(to)
        
        return exceptions
    end
    
    class GHAException < Exception
    end
end

class OnlineGHAProvider < GHAProvider
    def initialize(max_retries = 3, proactive = false, proactive_pool_size = 10)
        super()
        
        self.max_retries(max_retries)
        self.proactive(proactive_pool_size) if proactive
        
        @cache = Cache.new
    end
    
    def max_retries(n)
        @max_retries = n
        
        return self
    end
    
    def proactive(pool_size = 10)
        @proactive = true
        @pool = GHArchive::ThreadPool.new(pool_size)
        
        return self
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

                    data = @cache.get(filename)
                    if data
                        return data
                    else
                        raise DownloadArchiveException, "Could not scan #{filename}: data unavailable."
                    end
                else
                    URI.open("http://data.gharchive.org/#{filename}") do |gz|
                        return self.read_gha_file(gz)
                    end
                end
            rescue Errno::ECONNRESET => e
                @logger.warn("A server error temporary prevented the download of #{current_time}: " + e.message)
                next
            rescue OpenURI::HTTPError => e
                code = e.io.status[0]
                if code.start_with?("5")
                    @logger.warn("A server error temporary prevented the download of #{current_time}: " + e.message)
                    next
                else
                    raise e
                end
            end
        end
        
        raise DownloadArchiveException, "Exceeded maximum number of tentative downloads for #{current_time}."
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
            rescue Errno::ECONNRESET => e
                @logger.warn("A server error temporary prevented the download of #{current_time}: " + e.message)
                next
            rescue OpenURI::HTTPError => e
                code = e.io.status[0]
                if code.start_with?("5")
                    @logger.warn("A server error temporary prevented the download of #{current_time}: " + e.message)
                    next
                elsif code == "404"
                    @logger.error("File for #{current_time} not found. Skipping because: " + e.message)
                else
                    raise e
                end
            rescue Zlib::GzipFile::Error => e
                @logger.warn("Could not unzip, cache and analyze the zip at #{current_time}: " + e.message)
            end
        end
        
        @cache.put(filename, nil) unless @cache.has?(filename)
    end
    
    def each(from = Time.gm(2015, 1, 1), to = Time.now)
        if @proactive
            real_from = restore_checkpoint(from)
            any_ready = Thread.promise
            
            @logger.info("Proactively scheduling download tasks...")
            self.each_time(real_from, to) do |current_time|
                @pool.process(current_time) do |current_time|
                    cache(current_time)
                    any_ready << true
                    @logger.info("Proactively cached #{current_time}. Cache size: #{@cache.size}")
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
    
    class DownloadArchiveException < GHAProvider::GHAException
    end
end

class FolderGHAProvider < GHAProvider
    def initialize(folder)
        super()
        
        @folder = folder
    end
    
    def get(current_time)        
        filename = self.get_gha_filename(current_time)
        complete_filename = File.join(@folder, filename)
        mode = "rb"
        
        unless FileTest.exist?(complete_filename)
            complete_filename = complete_filename.sub(".gz", "")
            mode = "r"
        end
        
        unless FileTest.exist?(complete_filename)
            raise GHAException.new("Cannot find any file (neither `.json.gz` nor `.json`) for #{current_time}")
        end
        
        File.open(complete_filename, mode) do |file|
            return self.read_gha_file(file)
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
