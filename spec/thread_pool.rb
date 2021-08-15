require_relative File.expand_path("../../lib/gh-archive", __FILE__)
require 'rspec/autorun'

describe GHArchive::ThreadPool do
    it 'should complete all the jobs' do
        pool = GHArchive::ThreadPool.new(10)
        completed = 0
        
        100.times do
            pool.process do
               completed += 1
            end
        end
        pool.shutdown
        pool.wait
        
        expect(completed).to eq 100
    end
    
    it 'should correctly pass the arguments' do
        pool = GHArchive::ThreadPool.new(3)
        completed = 0
        
        pool.process("hello") do |v|
            expect(v).to eq "hello"
        end
        
        pool.process(42, nil, :test) do |a, b, c|
            expect(a).to eq 42
            expect(b).to be_nil
            expect(c).to eq :test
        end
        
        pool.shutdown
        pool.wait
    end
end
