# GitHub Archive Utils
[![Gem Version](https://badge.fury.io/rb/gh-archive.svg)](https://badge.fury.io/rb/gh-archive)

This gems helps mining GitHub Archive, without necessarily downloading the whole archive.

## Install
To install the latest version, simply run the following command

```
gem install gh-archive
```

## Examples

**Download the archive**
```ruby
require 'gh-archive'

# Download the 2015 archive in the "gz" folder
GHADownloader.new("gz").download(Time.gm(2015, 1, 1), Time.gm(2015, 12, 31))

# Download the decompressed files for the 2018 archive in the "jsons" folder
GHADownloader.new("jsons", false).download(Time.gm(2018, 1, 1), Time.gm(2018, 12, 31))

# Download the 2015 archive in the "temp" folder, keeps only the most recent 100 files
GHADownloader.new("temp").max(100).download(Time.gm(2015, 1, 1), Time.gm(2015, 12, 31)) do |latest|
    # do things
end
```

**Mining**
```ruby
require 'gh-archive'

provider = OnlineGHAProvider.new

# Only considers push events with a payload
provider.include(type: 'PushEvent')
provider.exclude(payload: nil)

# Prints the names of the authors of the commits of each push, separated by a comma
provider.each(Time.gm(2015, 1, 1), Time.gm(2015, 12, 31)) do |event|
    puts event['payload']['commits'].map { |c| c['author']['name']}.uniq.join(", ")
end
```
