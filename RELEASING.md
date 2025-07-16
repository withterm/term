# Release Process

This document describes how to release Term to crates.io.

## Overview

Term uses GitHub Releases to trigger automated publication to crates.io. When you create a GitHub release, our automated workflow handles all validation and publication steps.

## Before You Release

1. **Update version** in `term-guard/Cargo.toml`
2. **Update CHANGELOG.md** with release notes (optional - you can use GitHub's auto-generated notes)
3. **Merge all changes** to the main branch
4. **Ensure CI is green** on the main branch

## Creating a Release

1. Go to [Releases](https://github.com/withterm/term/releases) → **"Draft a new release"**
2. Click **"Choose a tag"** → Create new tag `v0.0.2` (use your version with `v` prefix)
3. Set **Release title**: `v0.0.2` (or add a codename like `v0.0.2 - Lightning`)
4. **Generate release notes**:
   - Click "Generate release notes" to auto-populate from PRs
   - Or manually write/paste your release notes
   - Or copy from CHANGELOG.md if you maintain one
5. If this is a test release, check **"Set as a pre-release"**
6. Click **"Publish release"**

## What Happens Next

Once you publish the release, GitHub Actions automatically:

1. **Validates** the release (version format, Cargo.toml match)
2. **Runs** all tests and checks
3. **Publishes** to crates.io (skip for pre-releases)
4. **Updates** your release with publication links

Monitor progress in the [Actions tab](https://github.com/withterm/term/actions).

## Pre-releases

Use pre-releases to test the release process without publishing to crates.io:

- Check **"Set as a pre-release"** when creating the release
- Pre-releases run all validations but skip crates.io publication
- Users can test via git dependency:

```toml
[dependencies]
term-guard = { git = "https://github.com/withterm/term", tag = "v0.0.2-beta.1" }
```

## Version Format

- Tags must be formatted as `vX.Y.Z` (e.g., `v0.0.2`, `v1.0.0`)
- Version in `Cargo.toml` must match (without the `v` prefix)
- We follow [Semantic Versioning](https://semver.org/)

## Required Setup

### GitHub Secret

Add your crates.io token to GitHub:

1. Get your token from https://crates.io/me
2. Go to Settings → Secrets and variables → Actions
3. Add secret named `CARGO_REGISTRY_TOKEN` with your token

### Permissions

You need:

- Write access to the Term repository
- Publish rights on crates.io for `term-guard`

## Troubleshooting

If the release workflow fails:

1. Check the [Actions tab](https://github.com/withterm/term/actions) for errors
2. Common issues:
   - **Version mismatch**: Tag `v0.0.2` must match Cargo.toml version `0.0.2`
   - **Missing secret**: Ensure `CARGO_REGISTRY_TOKEN` is set
   - **Test failures**: Fix tests and create a new release
3. Fix issues and create a new release (delete the failed one if needed)

## Emergency: Yanking a Release

If you published a broken version:

```bash
# Prevent new downloads (doesn't delete)
cargo yank --version 0.0.2 -p term-guard

# Undo if fixed
cargo yank --version 0.0.2 --undo -p term-guard
```

## That's It!

The entire release process is:

1. Update version number
2. Create GitHub release
3. Let automation handle the rest

Simple, consistent, and reliable.
