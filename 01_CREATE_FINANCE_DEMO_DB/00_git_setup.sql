USE ROLE ACCOUNTADMIN;

-- GitHubのアクセストークンを格納
CREATE OR REPLACE SECRET my_github_token
    TYPE = PASSWORD
    USERNAME = 'my_github_username'
    PASSWORD = 'ghp_xxxxxxxxxxxxxxxxx'; -- ここにPATを入れる

-- API統合を作成
CREATE OR REPLACE API INTEGRATION my_git_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/my-org') -- 組織やユーザーのURL
    ALLOWED_AUTHENTICATION_SECRETS = (my_github_token) -- ★ここでSECRETを使用
    ENABLED = TRUE;

-- Gitリポジトリを作成
CREATE OR REPLACE GIT REPOSITORY my_repo
    API_INTEGRATION = my_git_api_integration
    GIT_CREDENTIALS = my_github_token -- ★重要：プライベートリポジトリの場合は必須
    ORIGIN = 'https://github.com/my-org/my-project-repo.git'; -- リポジトリURL
