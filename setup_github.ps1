param(
    [string]$RepoUrl = '',
    [string]$Branch = 'main'
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
    throw 'Git is not installed or not on PATH. Install Git for Windows first: https://git-scm.com/download/win'
}

if (-not (Test-Path '.\quote_checker_app.py')) {
    throw 'Run this script from the quote-checker-app folder.'
}

if (-not (Test-Path '.\.git')) {
    git init
}

git add .

$status = git status --porcelain
if ($status) {
    git commit -m 'Initial commit - quote checker app'
} else {
    Write-Host 'No changes to commit.'
}

# Ensure branch name
try {
    git branch -M $Branch
} catch {
    Write-Host 'Branch rename skipped.'
}

if ($RepoUrl) {
    $existingOrigin = git remote get-url origin 2>$null
    if (-not $existingOrigin) {
        git remote add origin $RepoUrl
    } else {
        git remote set-url origin $RepoUrl
    }

    git push -u origin $Branch
    Write-Host "Pushed to $RepoUrl on branch $Branch"
} else {
    Write-Host 'Local repository is ready. Pass -RepoUrl to push to GitHub.'
}
