# Claude Code Configuration

This document defines Claude Code-specific settings, project context, and development guidance for this repository.

## Overview

This is a Tennessee Department of Agriculture (TDA) GIS template repository. All code should follow TDA data classification and security guidelines outlined in `AGENTS.md`.

## Claude Code Project Setup

### Python Environment

- **Python Version:** 3.11 (as specified in `.devcontainer/devcontainer.json`)
- **Package Manager:** pip
- **Dev Tools:** ruff (linting), pytest (testing when applicable)

Install dependencies:
```bash
pip install -r requirements.txt          # Production dependencies
pip install -r requirements-dev.txt      # Development dependencies (includes JupyterLab)
```

### Spatial Data Handling

This template works with geospatial data via `geopandas`, `arcgis` (Python API), and ArcGIS Online/Enterprise portals.

**Never commit large binary spatial files** (`.shp`, `.gdb`, `.tif`, `.gpkg`, etc.). See `.gitignore` for the full blocklist.

For working with large or binary spatial datasets:
- Store raw data in `data/raw/` (git-ignored)
- Store processed outputs in `data/processed/` (git-ignored)
- Document schema, CRS, and data transformations in notebooks or `docs/`
- If data context is needed, ask the user to provide a schema summary or sample metadata

### Code Organization

```
├── data/                          # Data storage (git-ignored)
│   ├── processed/                 # Cleaned, derived spatial outputs
│   └── raw/                       # Original downloads (DO NOT COMMIT)
├── docs/                          # Documentation, schemas, references
├── notebooks/                     # Jupyter notebooks for exploration
├── src/                           # Production Python code (if applicable)
├── scripts/                       # Production-ready scripts
├── .devcontainer/                 # VS Code dev container config
├── .env.example                   # Template for environment variables
├── .gitignore                     # Git ignore rules (spatial binaries, secrets)
├── AGENTS.md                      # AI agent constraints and guidelines
├── CLAUDE.md                      # Claude Code configuration (this file)
├── README.md                      # Project overview
├── requirements.txt               # Production dependencies
└── requirements-dev.txt           # Development dependencies
```

## Environment Variables

Use `.env.example` as a template. Create a `.env` file locally (git-ignored) with:
- ArcGIS Online username and password
- API keys (if applicable)
- Portal URLs or service names
- Any credentials or sensitive configuration

**Never commit actual credentials to Git.**

## AI Agent Notes

This template prioritizes working with Claude Code and other AI coding assistants. Refer to `AGENTS.md` for:
- Repository data classification and confidentiality rules
- Constraints on shell execution and direct file I/O
- Validation commands and testing expectations
- ArcGIS Online and Enterprise workflow safety guardrails

### For Claude Code Users

Claude Code can read, edit, and write files directly. Key guidelines:
1. Use existing code patterns and style as a reference
2. Keep changes minimal and focused on the user's request
3. Validate syntax and logic before reporting completion
4. Update `README.md` "Last Updated" field if making significant changes
5. Refer to `AGENTS.md` for data handling, networking, and GIS-specific constraints

## Development Workflow

### Running Code

- **Notebooks:** Use JupyterLab (in `requirements-dev.txt`)
  ```bash
  jupyter lab
  ```
  
- **Scripts:** Run Python scripts directly or via scheduled tasks
  ```bash
  python scripts/my_script.py
  ```

- **Testing:** When tests exist, run via pytest
  ```bash
  pytest
  ```

### Code Quality

- **Linting:** Use ruff for Python linting
  ```bash
  ruff check .
  ```

- **Formatting:** Code should follow PEP 8 conventions (ruff enforces this)

### Editing and Validation

When Claude Code or other agents edit files:
1. Review changes for correctness
2. Run validation commands when available (ruff, pytest)
3. Manually review Markdown, JSON, and YAML syntax if tools are unavailable
4. Test changes in notebooks or scripts before committing

## Spatial Data Practices

- **Coordinate Reference System (CRS):** Explicitly state CRS for all spatial operations
- **Geometry Validation:** Check for null geometries, self-intersections, or invalid coordinates before processing
- **Large Files:** Use `geopandas` chunking, spatial indexing, or filtering for datasets > 100 MB
- **Projections:** Never assume a CRS; verify against metadata, shapefiles, or GeoJSON properties
- **Comments:** Add brief comments for non-obvious spatial joins, overlays, buffers, or CRS transformations

## ArcGIS Online / Enterprise Workflows

Before modifying hosted layers, feature services, or scheduled jobs:
1. Confirm the target portal URL and account
2. Identify item IDs and service names
3. Prefer dry-run behavior for new scripts
4. Never overwrite services or truncate data without explicit user confirmation

Refer to `AGENTS.md` → "ArcGIS Online and Enterprise Safety" for full guardrails.

## Networking & Security

- **Corporate Proxy:** TDA team members operate behind Zscaler proxy
- **TLS/SSL:** Do not disable certificate verification in production scripts
- **External Requests:** Configure your Python scripts to trust local Zscaler certificates if needed

See `AGENTS.md` → "Script Networking Constraints" for details.

## Validation & Testing

### Manual Review (When Tools Unavailable)
- Markdown: check heading levels, links, and list indentation
- JSON/YAML/TOML: validate syntax, indentation, and required fields
- Python: inspect imports, naming, indentation, and side effects

### Automated Checks (When Available)
- Markdown linting: (no default tool configured; review manually)
- Python linting: `ruff check .`
- Python testing: `pytest` (run if tests exist)

## Getting Help

- **Claude Code Documentation:** `/help` in Claude Code
- **Project Questions:** Refer to `AGENTS.md` or `README.md`
- **Bug Reports:** Report issues at [claude-code-issues](https://github.com/anthropics/claude-code/issues)

## Last Updated

2026-09-16
