param(
    [string]$Registry = "memforestexampleacr",
    [string]$Repository = "azure-memforest-api",
    [string]$BaseImage = "${Registry}.azurecr.io/base/python:3.11-bookworm"
)

$ErrorActionPreference = "Stop"

$dirty = git status --porcelain
if ($dirty) {
    Write-Error "Refusing to build ACR image from a dirty working tree. Commit or stash changes first."
}

$sha = git rev-parse --short=12 HEAD
$image = "${Repository}:${sha}"

az acr build `
    --registry $Registry `
    --image $image `
    --build-arg BASE_IMAGE=$BaseImage `
    --build-arg GIT_SHA=$sha `
    --build-arg IMAGE_DIRTY=false `
    .

Write-Output "${Registry}.azurecr.io/${image}"
