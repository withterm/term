# Release Process

This document describes the process for releasing Term to crates.io.

## Prerequisites

1. **Cargo account**: Create an account at https://crates.io
2. **API Token**: Get your API token from https://crates.io/me
3. **Login**: Run `cargo login` and enter your API token
4. **Ownership**: Ensure you have publish rights for the `term-core` crate

## Pre-release Checklist

- [ ] All tests pass: `cargo test --all-features`
- [ ] Linting passes: `cargo clippy -- -D warnings`
- [ ] Documentation builds: `cargo doc --no-deps --all-features`
- [ ] Examples work: `cargo run --example basic_validation`
- [ ] Version updated in `Cargo.toml`
- [ ] CHANGELOG.md updated with release notes
- [ ] README.md reviewed and up to date

## Release Steps

### 1. Final Verification

```bash
# Run full test suite
cargo test --all-features

# Check for any unpublished dependencies
cargo publish --dry-run -p term-core

# Build documentation
cargo doc --no-deps --all-features --open
```

### 2. Tag the Release

```bash
# Create and push a tag
git tag -a v0.0.1 -m "Release v0.0.1"
git push origin v0.0.1
```

### 3. Publish to crates.io

```bash
# Publish term-core
cd term-core
cargo publish

# Wait for crates.io to process (usually ~1 minute)
# Verify at https://crates.io/crates/term-core
```

### 4. Create GitHub Release

1. Go to https://github.com/term/term/releases/new
2. Select the tag `v0.0.1`
3. Title: "Term v0.0.1 - Initial Release"
4. Copy release notes from CHANGELOG.md
5. Attach any binary artifacts if applicable
6. Publish release

### 5. Post-release

- [ ] Verify crate appears on crates.io
- [ ] Check documentation on docs.rs
- [ ] Test installation: `cargo install term-core`
- [ ] Update Linear ticket to "Done"
- [ ] Announce release (if applicable)

## Troubleshooting

### Common Issues

1. **"already uploaded"**: The version already exists. Bump version and try again.
2. **"missing metadata"**: Check Cargo.toml has all required fields.
3. **"authentication required"**: Run `cargo login` with your API token.
4. **"rate limit"**: Wait a few minutes and try again.

### Yanking a Release

If you need to yank a broken release:

```bash
cargo yank --version 0.0.1 -p term-core
```

To un-yank:

```bash
cargo yank --version 0.0.1 --undo -p term-core
```

## Version Numbering

We follow [Semantic Versioning](https://semver.org/):

- MAJOR version for incompatible API changes
- MINOR version for backwards-compatible functionality additions
- PATCH version for backwards-compatible bug fixes

For pre-1.0 releases:
- 0.0.x - Initial development, API may change
- 0.x.0 - API stabilizing, minor breaking changes possible
- 1.0.0 - First stable release, API compatibility guaranteed