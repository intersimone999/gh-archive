require_relative 'core'

GHAUtils = GHArchive::Utils

class GHAProvider < GHArchive::Provider
    def initialize(*args)
        warn "GHAProvider is deprecated. Please use GHArchive::Provider instead."
        super
    end
end

class OnlineGHAProvider < GHArchive::OnlineProvider
    def initialize(*args)
        warn "OnlineGHAProvider is deprecated. Please use GHArchive::OnlineProvider instead."
        super
    end
end

class FolderGHAProvider < GHArchive::FolderProvider
    def initialize(*args)
        warn "FolderGHAProvider is deprecated. Please use GHArchive::FolderProvider instead."
        super
    end
end

class GHADownloader < GHArchive::Downloader
    def initialize(*args)
        warn "FolderGHAProvider is deprecated. Please use GHArchive::FolderProvider instead."
        super
    end
end
