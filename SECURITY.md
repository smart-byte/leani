# Leani security policy

This project parses untrusted network and dataset input and must be treated as
security-sensitive even when deployed as a private indexer.

Do not open a public issue for a suspected vulnerability. Until a dedicated
security contact exists, send a private GitHub security advisory to the
repository owner. Include the affected revision, deployment assumptions, and a
minimal non-destructive reproduction description.

Only the latest tagged release and `main` receive security fixes before v1.
Operators must bind APIs to loopback or a trusted network, enforce the
configured resource budgets, and avoid putting credentials in TOML files.
