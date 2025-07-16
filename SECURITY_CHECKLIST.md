# Security Checklist for Open-Sourcing Term

This checklist tracks the implementation of security measures before making the repository public.

## ✅ Completed Security Measures

### Workflow Security
- [x] **Concurrency controls**: Added to all workflows to prevent duplicate runs
- [x] **Timeout limits**: Set on all jobs (30-45 minutes max)
- [x] **Minimal permissions**: Default read-only permissions on all workflows
- [x] **Environment protection**: Added `production` environment for publish workflow

### Repository Protection
- [x] **CODEOWNERS file**: Created to protect .github/workflows/ directory
- [x] **Dependabot configuration**: Set up for automated dependency updates
- [x] **Security policy**: Created .github/security.md for vulnerability reporting
- [x] **Label-based PR approval**: Added pr-labeled.yml for fork PR testing

### Existing Security Features
- [x] **Security scanning workflow**: Comprehensive security.yml with:
  - cargo-audit for vulnerability scanning
  - cargo-deny for license compliance
  - GitLeaks for secret detection
  - Enhanced Clippy security lints
- [x] **Rust security**: deny.toml configured with approved licenses
- [x] **Cache optimization**: Using Swatinem/rust-cache@v2

## ⚠️ Required GitHub Settings (Manual Configuration)

### Before Going Public - Critical Settings

1. **Repository Settings → Actions → General**:
   - [ ] Set "Require approval for first-time contributors who are new to GitHub"
   - [ ] Keep "Read repository contents and packages permissions" for fork PRs
   - [ ] Set default permissions to "Read repository contents permission"

2. **Repository Settings → Environments**:
   - [ ] Create "production" environment
   - [ ] Add required reviewers (yourself)
   - [ ] Enable "Required reviewers" protection rule

3. **Repository Settings → Branches**:
   - [ ] Add branch protection rule for `main`:
     - [ ] Require PR reviews (1-2 reviewers)
     - [ ] Dismiss stale PR reviews when new commits are pushed
     - [ ] Require status checks: CI, PR Validation, Security Audit
     - [ ] Require branches to be up to date before merging
     - [ ] Include administrators in restrictions
     - [ ] Restrict who can push to matching branches

4. **Organization Settings → Actions → General** (if applicable):
   - [ ] Set spending limit to $0
   - [ ] Enable "Limit actions and reusable workflows"
   - [ ] Configure allowed actions to specific list

5. **Repository Settings → Code security and analysis**:
   - [ ] Enable Dependabot alerts
   - [ ] Enable Dependabot security updates
   - [ ] Enable secret scanning
   - [ ] Enable push protection for secrets

## 📋 Optional but Recommended Enhancements

### Advanced Monitoring (Medium Priority)
- [ ] Add StepSecurity Harden-Runner to workflows for runtime monitoring
- [ ] Implement sccache for faster builds (50%+ improvement)
- [ ] Set up workflow metrics dashboard

### Action Pinning (Medium Priority)
- [ ] Pin all actions to specific commit SHAs instead of tags
- [ ] Document the process for updating pinned actions

### Additional Security Hardening
- [ ] Add SECURITY.md to root (currently in .github/)
- [ ] Configure GitHub Security Advisories
- [ ] Set up CodeQL analysis for Rust (when available)

## 🚀 Pre-Launch Checklist

Before making the repository public:

1. [ ] Review all workflow files for sensitive information
2. [ ] Ensure no secrets/tokens are committed (run GitLeaks locally)
3. [ ] Verify branch protection rules are active
4. [ ] Test the 'safe-to-test' label workflow with a test PR
5. [ ] Confirm production environment requires approval
6. [ ] Set repository spending limits to $0
7. [ ] Enable all security features in repository settings
8. [ ] Review and update security contact email in security.md

## 📊 Monitoring After Launch

First Week:
- [ ] Monitor Actions usage daily for anomalies
- [ ] Review all first-time contributor PRs
- [ ] Check for any failed security scans
- [ ] Monitor Dependabot PR queue

First Month:
- [ ] Review security audit logs
- [ ] Update any vulnerable dependencies
- [ ] Assess if additional restrictions are needed
- [ ] Document any security incidents

## 🔍 Security Incident Response

If suspicious activity is detected:
1. Immediately set "Require approval for all outside collaborators"
2. Review recent workflow runs for unusual patterns
3. Check for cryptocurrency mining indicators (long-running jobs)
4. Disable problematic workflows if necessary
5. Report abuse to GitHub Support

## 📚 Additional Resources

- [GitHub Actions Security Hardening](https://docs.github.com/en/actions/security-guides/security-hardening-for-github-actions)
- [OpenSSF Scorecard](https://github.com/ossf/scorecard) - Run this after going public
- [SLSA Framework](https://slsa.dev/) - For supply chain security

---

**Note**: This checklist is based on best practices from major Rust projects including tokio, serde, and rust-lang/rust. Keep this document updated as security practices evolve.