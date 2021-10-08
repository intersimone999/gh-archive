require 'time'
require_relative 'core'

module GHArchive
    Repository = Struct.new(:id, :name, :url)
    CommitAuthor = Struct.new(:email, :name)
    
    class Entity
        def initialize(payload)
            @payload = payload
        end
    end
    
    class Commit < Entity
        def sha
            @payload['sha']
        end
        
        def author
            CommitAuthor.new(
                @payload['author']['email'],
                @payload['author']['name']
            )
        end
        
        def message
            @payload['message']
        end
        
        def distinct
            @payload['distinct']
        end
        
        def url
            @payload['url']
        end
    end
    
    class User < Entity
        def id
            @payload['id']
        end
        
        def url
            @payload['url']
        end
        
        def type
            @payload['type']
        end
        
        def login
            @payload['login']
        end
        
        def gravatar_id
            @payload['gravatar_id']
        end
        
        def avatar_url
            @payload['avatar_url']
        end
        
        def site_admin
            @payload['site_admin']
        end
    end
    
    class BasicIssue < Entity                
        def url
            @payload['url']
        end
        
        def id
            @payload['id']
        end
        
        def number
            @payload['number']
        end
        
        def state
            @payload['state']
        end
        
        def locked
            @payload['locked']
        end
        
        def title
            @payload['title']
        end
        
        def body
            @payload['body']
        end
        
        def user
            User.new(@payload['user']) rescue nil
        end
        
        def created_at
            Time.parse(@payload['created_at'])
        end
        
        def updated_at
            Time.parse(@payload['updated_at']) rescue nil
        end
        
        def closed_at
            Time.parse(@payload['closed_at']) rescue nil
        end
    end
    
    class PullRequest < BasicIssue        
        def merged_at
            Time.parse(@payload['merged_at']) rescue nil
        end
        
        def merge_commit_sha
            @payload['merge_commit_sha']
        end
                
        def merged
            @payload['merged']
        end
        
        def mergeable
            @payload['mergeable']
        end
        
        def mergeable_state
            @payload['mergeable_state']
        end
        
        def merged_by
            @payload['merged_by']
        end
        
        def comments
            @payload['comments']
        end
        
        def review_comments
            @payload['review_comments']
        end
        
        def commits
            @payload['commits']
        end
        
        def additions
            @payload['additions']
        end
        
        def deletions
            @payload['deletions']
        end
        
        def changed_files
            @payload['changed_files']
        end
        
        def head
            @payload['head']
        end
        
        def base
            @payload['base']
        end
    end
    
    class Issue < BasicIssue        
        def labels
            @payload['labels']
        end
    end
    
    class BasicComment < Entity        
        def url
            @payload['url']
        end
        
        def id
            @payload['id']
        end
        
        def user
            User.new(@payload['user']) rescue nil
        end
        
        def created_at
            Time.parse(@payload['created_at'])
        end
        
        def updated_at
            Time.parse(@payload['updated_at']) rescue nil
        end
        
        def body
            @payload['body']
        end
    end
    
    class PullRequestComment < BasicComment
        def diff_hunk
            @payload['diff_hunk']
        end
        
        def path
            @payload['path']
        end
        
        def position
            @payload['position']
        end
        
        def original_position
            @payload['original_position']
        end
        
        def commit_id
            @payload['commit_id']
        end
        
        def original_commit_id
            @payload['original_commit_id']
        end
    end
    
    class IssueComment < BasicComment
    end
    
    class Release < Entity
        def url
            @payload['url']
        end
        
        def id
            @payload['id']
        end
        
        def tag_name
            @payload['tag_name']
        end
        
        def target_commitish
            @payload['target_commitish']
        end
        
        def name
            @payload['name']
        end
        
        def draft
            @payload['draft']
        end
        
        def author
            User.new(@payload['author'])
        end
        
        def prerelease
            @payload['prerelease']
        end
        
        def created_at
            Time.parse(@payload['created_at'])
        end
        
        def published_at
            Time.parse(@payload['published_at'])
        end
        
        def assets
            @payload['assets']
        end
        
        def tarball_url
            @payload['tarball_url']
        end
        
        def zipball_url
            @payload['zipball_url']
        end
        
        def body
            @payload['body']
        end
    end
    
    class Page < Entity        
        def name
            @payload['page_name']
        end
        
        def title
            @payload['title']
        end
        
        def summary
            @payload['summary']
        end
        
        def action
            @payload['action']
        end
        
        def sha
            @payload['sha']
        end
    end
end
