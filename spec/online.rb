require_relative File.expand_path("../../lib/gh-archive", __FILE__)
require 'rspec/autorun'
require 'stringio'

describe OnlineGHAProvider do
    it "should work with archives existing on GitHub Archive" do
        provider = OnlineGHAProvider.new
        provider.max_retries(10)
        
        events = []
        dates = []
        
        exceptions = provider.each(Time.gm(2015,1,1,3), Time.gm(2015,1,1,6)) do |event, date|
            events << event
            dates << date
        end
        
        expect(events.size).to eq 17727
        expect(dates.uniq.size).to eq 3
        expect(exceptions.size).to eq 0
    end
    
    it "should work with archives existing on GitHub Archive also in proactive mode" do
        provider = OnlineGHAProvider.new
        provider.max_retries(10)
        provider.proactive(5)
        
        events = []
        dates = []
        
        exceptions = provider.each(Time.gm(2015,1,1,3), Time.gm(2015,1,1,6)) do |event, date|
            events << event
            dates << date
        end
        
        expect(events.size).to eq 17727
        expect(dates.uniq.size).to eq 3
        expect(exceptions.size).to eq 0
    end
    
    it "should proactively schedule by taking checkpoints into account" do
        provider = OnlineGHAProvider.new
        provider.max_retries(10)
        provider.proactive(5)
        
        f = Tempfile.new
        f.write(Marshal.dump(Time.gm(2015,1,1,5)))
        f.close
        
        logger = StringIO.new
        provider.logger = Logger.new(logger)
        
        provider.use_checkpoint(f.path)
        
        exceptions = provider.each(Time.gm(2015,1,1,3), Time.gm(2015,1,1,6)) do |event, date|
        end
        
        expect(logger.string).not_to match(/Proactively cached 2015\-01\-01 03\:00\:00/)
        expect(logger.string).not_to match(/Proactively cached 2015\-01\-01 04\:00\:00/)
        expect(logger.string).to match(/Proactively cached 2015\-01\-01 05\:00\:00/)
    end
    
    it "should work with archives existing on GitHub Archive also in extremely proactive mode" do
        provider = OnlineGHAProvider.new
        provider.max_retries(10)
        provider.proactive(100)
        
        events = []
        dates = []
        
        exceptions = provider.each(Time.gm(2015,1,1,3), Time.gm(2015,1,1,6)) do |event, date|
            events << event
            dates << date
        end
        
        expect(events.size).to eq 17727
        expect(dates.uniq.size).to eq 3
        expect(exceptions.size).to eq 0
    end
    
    it "should parse the file in the correct order" do
        provider = OnlineGHAProvider.new
        provider.max_retries(10)
        
        last_time = Time.at(0)
        exceptions = provider.each(Time.gm(2015,1,1,3), Time.gm(2015,1,1,6)) do |event, date|
            time = Time.parse(Time.parse(event['created_at']).strftime("%Y-%m-%d %H:00:00 +0000"))
            expect(last_time).to be <= time
            last_time = time
        end
    end
    
    it "should parse the file in the correct order also in proactive mode" do
        provider = OnlineGHAProvider.new
        provider.max_retries(10)
        provider.proactive(5)
        
        last_time = Time.at(0)
        exceptions = provider.each(Time.gm(2015,1,1,3), Time.gm(2015,1,1,6)) do |event, date|
            time = Time.parse(Time.parse(event['created_at']).strftime("%Y-%m-%d %H:00:00 +0000"))
            expect(last_time).to be <= time
            last_time = time
        end
    end
    
    it "should skip unexisting archives" do
        provider = OnlineGHAProvider.new
        provider.max_retries(10)
        
        events = []
        dates = []
        
        exceptions = provider.each(Time.gm(2011,2,11,3), Time.gm(2011,2,12,3)) do |event, date|
            events << event
            dates << date
        end
        
        expect(events.size).to eq 4337
        expect(dates.uniq.size).to eq 3
        expect(exceptions.size).to eq 21
    end
end
