require 'gh-archive'
require 'rspec/autorun'

describe OnlineGHAProvider::Cache do
    it 'should not leak memory' do
        cache = OnlineGHAProvider::Cache.new
        
        10.times do |i|
            free_memory_before_allocating = GC.stat[:heap_free_slots]
            cache.put("n#{i}", "abcdefghij" * 31457280) # 300M content
            free_memory_after_allocating = GC.stat[:heap_free_slots]
            cache.get("n#{i}")
            GC.start
            free_memory_after_free = GC.stat[:heap_free_slots]
            
            expect(free_memory_after_free).to be > free_memory_after_allocating
        end
    end
end
