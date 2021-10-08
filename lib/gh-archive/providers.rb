require_relative 'core'

module GHArchive
    class Provider
        include Utils
        
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

    class OnlineProvider < Provider
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
            
            filename = self.get_gha_filename(current_time)
            @max_retries.times do
                begin
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
                @mutex.synchronize do
                    return @cache.has_key?(name)
                end
            end
            
            def full?
                self.size >= @max_size
            end
        end
        
        class DownloadArchiveException < Provider::GHAException
        end
    end

    class FolderProvider < Provider
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
end
