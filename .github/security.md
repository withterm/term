# Security Policy

## Supported Versions

Currently, we support the following versions with security updates:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take the security of Term seriously. If you believe you have found a security vulnerability, please report it to us as described below.

### Please do NOT:
- Open a public GitHub issue for security vulnerabilities
- Include sensitive information in pull requests

### Please DO:
- Email security concerns to: [Add your security email here]
- Include the word "SECURITY" in the subject line
- Provide detailed steps to reproduce the issue
- Allow us reasonable time to address the issue before public disclosure

### What to expect:
1. **Acknowledgment**: We'll acknowledge receipt within 48 hours
2. **Assessment**: We'll assess the issue and determine its severity
3. **Fix Timeline**: We'll provide an estimated timeline for a fix
4. **Disclosure**: We'll coordinate disclosure timing with you

## Security Best Practices for Contributors

When contributing to Term, please follow these security guidelines:

1. **Dependencies**: 
   - Only add dependencies that are necessary
   - Prefer well-maintained crates with good security track records
   - Check dependencies using `cargo audit` before submitting PRs

2. **Code Practices**:
   - Never commit secrets, tokens, or credentials
   - Avoid using `unsafe` code unless absolutely necessary
   - Handle errors properly - avoid `unwrap()` in production code
   - Validate all inputs, especially when parsing external data

3. **Testing**:
   - Include tests for security-sensitive code paths
   - Test error conditions and edge cases
   - Run the full test suite before submitting PRs

## Security Features in Term

Term includes several security-focused features:

1. **Input Validation**: All data sources are validated before processing
2. **Safe Query Execution**: SQL injection prevention through parameterized queries
3. **Memory Safety**: Leverages Rust's memory safety guarantees
4. **Dependency Scanning**: Automated vulnerability scanning via GitHub Actions

## Acknowledgments

We appreciate responsible disclosure and will acknowledge security researchers who help us keep Term secure.