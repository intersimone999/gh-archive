require 'time'
require_relative 'entities'

module GHArchive
    class Event
        def self.parse(json)
            IMPLEMENTATIONS.each do |event_class|
                return event_class.new(json) if event_class.fits?(json)
            end
            
            return Event.new(json)
        end
        
        def initialize(json)
            @json = json.freeze
            @payload = json['payload']
        end
        
        def public?
            @json['public']
        end
        
        def created_at
            Time.parse(@json['created_at'])
        end
        alias :time :created_at
        
        def actor
            User.new(@json['actor'])
        end
        
        def repo
            Repository.new(
                @json['repo']['id'],
                @json['repo']['name'],
                @json['repo']['url']
            )
        end
        
        def json
            @json
        end
    end
    
    class PushEvent < Event        
        def self.fits?(json)
            json['type'] == "PushEvent"
        end
        
        def push_id
            @payload['push_id']
        end
        
        def size
            @payload['size']
        end
        
        def distinct_size
            @payload['distinct_size']
        end
        
        def head
            @payload['head']
        end
        
        def before
            @payload['before']
        end
        
        def commits
            @payload['commits'].map { |c| Commit.new(c) }
        end
    end
    
    class CommitCommentEvent < Event
        def self.fits?(json)
            return json['type'] == "CommitCommentEvent"
        end
        
        def comment_id
            @payload['comment']['id']
        end
        
        def comment_url
            @payload['comment']['url']
        end
        
        def comment_user
            User.new(@payload['comment']['author'])
        end
        
        def comment_position
            @payload['comment']['position']
        end
        
        def comment_line
            @payload['comment']['line']
        end
        
        def comment_path
            @payload['comment']['path']
        end
        
        def comment_commit_id
            @payload['comment']['commit_id']
        end
        
        def comment_body
            @payload['comment']['body']
        end
        
        def comment_created_at
            Time.parse(@payload['comment']['created_at'])
        end
        
        def comment_updated_at
            Time.parse(@payload['comment']['updated_at'])
        end
    end
    
    class PullRequestEvent < Event
        def self.fits?(json)
            return json['type'] == "PullRequestEvent"
        end
        
        def action
            @payload['action']
        end
        
        def number
            @payload['number']
        end
        
        def pull_request
            PullRequest.new(@payload['pull_request'])
        end
    end
    
    class PullRequestReviewCommentEvent < Event
        def self.fits?(json)
            return json['type'] == "PullRequestReviewCommentEvent"
        end
        
        def action
            @payload['action']
        end
        
        def number
            @payload['number']
        end
        
        def pull_request
            PullRequest.new(@payload['pull_request'])
        end
        
        def comment
            PullRequestComment.new(@payload['comment'])
        end
    end
    
    class IssuesEvent < Event
        def self.fits?(json)
            return json['type'] == "IssuesEvent"
        end
        
        def action
            @payload['action']
        end
        
        def issue
            Issue.new(@payload['issue'])
        end
    end
    
    class IssueCommentEvent < Event
        def self.fits?(json)
            return json['type'] == "IssueCommentEvent"
        end
        
        def action
            @payload['action']
        end
        
        def issue
            Issue.new(@payload['issue'])
        end
    end
    
    class CreateEvent < Event
        def self.fits?(json)
            return json['type'] == "CreateEvent"
        end
        
        def ref
            @payload['ref']
        end
        
        def ref_type
            @payload['ref_type']
        end
        
        def master_branch
            @payload['master_branch']
        end
        
        def description
            @payload['description']
        end
        
        def pusher_type
            @payload['pusher_type']
        end
    end
    
    class ForkEvent < Event
        def self.fits?(json)
            return json['type'] == "ForkEvent"
        end
        
        def forkee_id
            @payload['forkee']['id']
        end
        
        def forkee_name
            @payload['forkee']['name']
        end
        
        def forkee_full_name
            @payload['forkee']['full_name']
        end
        
        def forkee_owner
            User.new(@payload['forkee']['owner'])
        end
        
        def forkee_private
            @payload['forkee']['private']
        end
        
        def forkee_description
            @payload['forkee']['description']
        end
        
        def forkee_fork
            @payload['forkee']['fork']
        end
        
        def forkee_created_at
            Time.parse(@payload['forkee']['created_at'])
        end
        
        def forkee_updated_at
            Time.parse(@payload['forkee']['updated_at'])
        end
        
        def forkee_pushed_at
            Time.parse(@payload['forkee']['pushed_at'])
        end
        
        def forkee_urls
            {
                'git' => @payload['forkee']['git_url'],
                'ssh' => @payload['forkee']['ssh_url'],
                'clone' => @payload['forkee']['clone_url'],
                'svn' => @payload['forkee']['svn_url']
            }
        end
        
        def forkee_homepage
            Time.parse(@payload['forkee']['homepage'])
        end
        
        def forkee_size
            Time.parse(@payload['forkee']['size'])
        end
        
        def forkee_stargazers_count
            Time.parse(@payload['forkee']['stargazers_count'])
        end
        
        def forkee_watchers_count
            Time.parse(@payload['forkee']['watchers_count'])
        end
        
        def forkee_language
            Time.parse(@payload['forkee']['language'])
        end
        
        def forkee_has_issues
            Time.parse(@payload['forkee']['has_issues'])
        end
        
        def forkee_has_downloads
            Time.parse(@payload['forkee']['has_downloads'])
        end
        
        def forkee_has_wiki
            Time.parse(@payload['forkee']['has_wiki'])
        end
        
        def forkee_has_pages
            Time.parse(@payload['forkee']['has_pages'])
        end
        
        def forkee_forks_count
            Time.parse(@payload['forkee']['forks_count'])
        end
        
        def forkee_mirror_url
            Time.parse(@payload['forkee']['mirror_url'])
        end
        
        def forkee_open_issues_count
            Time.parse(@payload['forkee']['open_issues_count'])
        end
        
        def forkee_watchers
            Time.parse(@payload['forkee']['watchers'])
        end
        
        def forkee_default_branch
            Time.parse(@payload['forkee']['default_branch'])
        end
        
        def forkee_public
            Time.parse(@payload['forkee']['public'])
        end
    end
    
    class PublicEvent < Event
        def self.fits?(json)
            return json['type'] == "PublicEvent"
        end
    end
    
    class WatchEvent < Event
        def self.fits?(json)
            return json['type'] == "WatchEvent"
        end
        
        def action
            @payload['action']
        end
    end
    
    class DeleteEvent < Event
        def self.fits?(json)
            return json['type'] == "DeleteEvent"
        end
        
        def ref
            @payload['ref']
        end
        
        def ref_type
            @payload['ref_type']
        end
                
        def pusher_type
            @payload['pusher_type']
        end
    end
    
    class ReleaseEvent < Event
        def self.fits?(json)
            return json['type'] == "ReleaseEvent"
        end
        
        def action
            @payload['action']
        end
        
        def release
            Release.new(@payload['release'])
        end
    end
    
    class MemberEvent < Event
        def self.fits?(json)
            return json['type'] == "MemberEvent"
        end
        
        def action
            @payload['action']
        end
        
        def member
            User.new(@payload['member'])
        end
    end
    
    class GollumEvent < Event
        def self.fits?(json)
            return json['type'] == "GollumEvent"
        end
        
        def pages
            @payload[pages].map { |p| Page.new(p) }
        end
    end
    
    class Event
        IMPLEMENTATIONS = ObjectSpace.each_object(Class).select { |klass| klass < self }
    end
end
