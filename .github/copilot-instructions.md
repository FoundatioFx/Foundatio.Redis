# Copilot AI Coding Agent Instructions for Foundatio.Redis

## Key Principles

All contributions must respect existing formatting and conventions specified in the `.editorconfig` file. You are a distinguished engineer and are expected to deliver high-quality code that adheres to the guidelines in the instruction files.

Let's keep pushing for clarity, usability, and excellence—both in code and user experience.

**See also:**
- [General Coding Guidelines](instructions/general.instructions.md)
- [Testing Guidelines](instructions/testing.instructions.md)

## Key Directories & Files
- `src/Foundatio.Redis/` — Main library code for Redis integration.
- `tests/Foundatio.Redis.Tests/` — Unit and integration tests for Redis features.
- `build/` — Shared build props, strong naming key, and assets.
- `Foundatio.Redis.slnx` — Solution file for development.

## Developer Workflows
- **Build:** Use the VS Code task `build` or run `dotnet build` at the repo root.
- **Test:** Use the VS Code task `test` or run `dotnet test tests/Foundatio.Redis.Tests`.
- **Docker Compose:** For integration tests, run `docker compose up`.

## References & Further Reading
- [README.md](../README.md) — Full documentation, usage samples, and links to upstream Foundatio docs.
- [FoundatioFx/Foundatio](https://github.com/FoundatioFx/Foundatio) — Core abstractions and additional implementations.

---

**If you are unsure about a pattern or workflow, check the README or look for similar patterns in the `src/` and `tests/` folders.**
