# Security

This is a reference sample, not a supported product. Still, if you spot a security
issue in the code (for example, a pattern that could leak credentials), please report
it responsibly.

- **Do not** open a public issue for a sensitive vulnerability.
- Instead, contact the repository owner privately via GitHub.

## Handling credentials when using this project

- Never commit a real `.env` or `.env.demo`. Both are gitignored; only the
  `*.example` templates are tracked.
- Prefer a named `~/.snowflake/connections.toml` entry or a secret store over putting
  secrets in files.
- For unattended jobs, use key-pair (RSA) authentication rather than a long-lived
  password or token.
- The demo passwords (`demopw` for MySQL, `Demo_Passw0rd` for the SQL Server `sa`
  login) are throwaways for local containers only and are never exposed off localhost.
