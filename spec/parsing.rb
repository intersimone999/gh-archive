require 'gh-archive'
require 'rspec/autorun'

describe GHAProvider do
    it "should parse events when asked for" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
        events = []
        dates = []
        
        exceptions = provider.parse_events.each(Time.gm(2015,1,1,3), Time.gm(2015,1,1,6)) do |event, date|
            events << event
        end
        
        classes = events.map { |e| e.class }.uniq
        expect(classes).not_to include Hash
        expect(classes).not_to include GHArchive::Event
    end
    
    it "should only contain unparsed events by default" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
        events = []
        dates = []
        
        exceptions = provider.each(Time.gm(2015,1,1,3), Time.gm(2015,1,1,6)) do |event, date|
            events << event
            dates << date
        end
        
        classes = events.map { |e| e.class }.uniq
        expect(classes).to eq [Hash]
    end
    
    it "should correctly parse all the types of events and entities" do
        provider = FolderGHAProvider.new("#{File.dirname(File.expand_path($0))}/test_folder")
        
        events = []
        dates = []
        
        exceptions = provider.parse_events.each(Time.gm(2015,1,1,3), Time.gm(2015,1,1,6)) do |event, date|
            events << event
        end
        
        classes = events.map { |e| e.class }.uniq
        
        classes.each do |event_type|
            sample = events.first { |e| e.class == event_type }
            (sample.methods - Object.methods).each do |method|
                result = sample.method(method).call
                expect(result).not_to be_nil
                
                if result.is_a?(GHArchive::Entity)
                    (result.methods - Object.methods).each do |method|
                        result2 = result.method(method).call
                    end
                end
            end
        end
    end
end
