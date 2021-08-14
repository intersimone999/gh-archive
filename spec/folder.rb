require_relative File.expand_path("../../lib/gh-archive", __FILE__)
require 'rspec/autorun'

describe FolderGHAProvider do
    it "should process all the compressed files" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
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
    
    it "should process all the uncompressed files" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
        events = []
        dates = []
        
        exceptions = provider.each(Time.gm(2015,1,2,0), Time.gm(2015,1,2,3)) do |event, date|
            events << event
            dates << date
        end
        
        expect(events.size).to eq 30708
        expect(dates.uniq.size).to eq 3
        expect(exceptions.size).to eq 0
    end
    
    it "should skip unexisting files" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
        events = []
        dates = []
        
        exceptions = provider.each(Time.gm(2015,1,2,0), Time.gm(2015,1,2,5)) do |event, date|
            events << event
            dates << date
        end
        
        expect(events.size).to eq 30708
        expect(dates.uniq.size).to eq 3
        expect(exceptions.size).to eq 0
    end
    
    it "should skip compromised gz files" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
        dates = []
        
        exceptions = provider.each(Time.gm(2015,1,1,11), Time.gm(2015,1,1,12)) do |event, date|
            dates << date
        end
                
        expect(dates.uniq.size).to eq 0
        expect(exceptions.size).to eq 1
        expect(exceptions[0].message).to match(/.*not in gzip format.*/)
    end
end
