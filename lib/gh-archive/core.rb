require 'code-assertions'
require 'json'
require 'open-uri'
require 'zlib'
require 'logger'
require 'tmpdir'
require 'thread/pool'
require 'thread/promise'

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
    
    module Utils
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
end
