# Purpose

Code generation system that reads a core API definition and generates the three
client surfaces (CLI subcommands, HTTP routes, MCP tool definitions) from it.
This ensures all surfaces stay in sync — adding an operation to the definition
automatically generates code in all three targets.

# Requirements

### Requirement: Single API Definition Source

The system must maintain a single API definition file that describes all
operations Iris exposes.

- Operations are defined with: name, description, input parameters, output type.
- Core operations: list_messages, list_threads, list_contacts, send_message.

### Requirement: CLI Surface Generation

The system must generate Clap subcommand structs from the API definition.

- Each operation becomes a subcommand with typed arguments.
- Output is JSON to stdout.

### Requirement: HTTP Surface Generation

The system must generate Axum route handlers from the API definition.

- Each operation becomes a REST endpoint.
- GET for read operations, POST for write operations.
- All routes return JSON.

### Requirement: MCP Surface Generation

The system must generate MCP tool definitions from the API definition.

- Each operation becomes an MCP tool.
- Tool input schema matches the API definition parameters.

### Requirement: Surface Consistency Guarantee

The system must enforce that all three surfaces (CLI, HTTP, MCP) are generated
from the same definition and are always in sync.

#### Scenario: Adding a new operation

When a new operation is added to the API definition and codegen runs, the CLI,
HTTP, and MCP surfaces all gain the new operation automatically.

#### Scenario: CI catches stale generated code

When generated code is out of date, CI fails with a diff showing what needs
regeneration.
