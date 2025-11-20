# E-Commerce Delta Lake Pipeline (DEV → UAT → PROD)

This project is a fully versioned, multi-environment Databricks pipeline with CI/CD.
- DEV → manual updates
- UAT & PROD → auto-updated only via PR merges
- Workflow deployment controlled by `config/databricks_config.json`
- Version handled inside `config/config.json`

## Branch Strategy
- feature/* → dev (PR required)
- dev → uat (PR)
- uat → prod (PR, approval required)

## Folders
- notebooks/ – actual notebooks
- src/ – libs & job modules
- config/ – environment & workflow config
- deploy.py – deploy script for GitHub Actions
