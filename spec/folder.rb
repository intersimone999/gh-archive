require_relative File.expand_path("../../lib/gh-archive", __FILE__)
require 'tempfile'
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
    
    it "should restore the execution when using checkpoints" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
        dates = []
        
        f = Tempfile.new
        path = f.path
        f.close
        
        File.unlink(path)
        provider.use_checkpoint(path)
        
        provider.each(Time.gm(2015,1,2,0), Time.gm(2015,1,2,3)) do |event, date|
            dates << date
            break if date.hour == 1
        end
                
        exceptions = provider.each(Time.gm(2015,1,2,0), Time.gm(2015,1,2,3)) do |event, date|
            dates << date
            expect(date.hour).to be >= 1
        end
        
        expect(dates.uniq.size).to eq 3
        expect(exceptions.size).to eq 0
    end
    
    it "should not analyze twice the last date when using checkpoints" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
        dates = []
        
        f = Tempfile.new
        path = f.path
        f.close
        
        File.unlink(path)
        provider.use_checkpoint(path)
        
        events1 = []
        exceptions = provider.each(Time.gm(2015,1,2,0), Time.gm(2015,1,2,3)) do |event, date|
            events1 << event
            dates << date
        end
        
        events2 = []
        exceptions += provider.each(Time.gm(2015,1,2,0), Time.gm(2015,1,2,3)) do |event, date|
            events2 << event
            dates << date
        end
        
        expect(events1.size).to eq 30708
        expect(events2.size).to eq 0
        expect(exceptions.size).to eq 0
    end
    
    it "should raise an exception when the file is not a checkpoint" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
        dates = []
        
        f = Tempfile.new
        f.write("ABC")
        f.close
        
        provider.use_checkpoint(f.path)
        
        expect {
            exceptions = provider.each(Time.gm(2015,1,2,0), Time.gm(2015,1,2,3)) {}
        }.to raise_error TypeError
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
