# SQL Column-Level Lineage Analyzer

## Overview

This tool analyzes T-SQL scripts to determine column-level data lineage. It parses SQL code using `Microsoft.SqlServer.TransactSql.ScriptDom`, builds an Abstract Syntax Tree (AST), and traverses it to identify how data flows between columns across different statements, tables, views, and Common Table Expressions (CTEs). The final output is a JSON representation of the lineage graph.

## Core Features

* Parses T-SQL scripts into Abstract Syntax Trees (ASTs) and builds column-level lineage
* Handles CTEs, aliases, joins, derived tables, INSERT/UPDATE/DELETE/MERGE/EXECUTE
* SSDT project analysis with cross-platform compatibility (parses .sqlproj and included .sql files)
* OpenLineage export with dataset schema, dataSource, and columnLineage facets
* Schema export/import for richer lineage (feeds dataset schemas into OpenLineage)
* Debugging tools for complex SQL: AST dump, missing-processor reporting, detailed logs

## Architecture

The solution follows a modular design:

*   **`ColumnLineageCli`**: The command-line interface project responsible for handling user input (SQL file path) and displaying the output.
*   **`ColumnLineageCore`**: The core analysis engine containing the parsing logic, AST traversal, processor implementations, and graph building components.
*   **`ColumnLineageCore.Tests`**: Contains unit and integration tests to verify the correctness of the analysis logic.

The core engine employs a **Processor-based design** adhering to SOLID principles:

*   It uses dedicated processors for different types of SQL fragments encountered in the AST.
*   An `IProcessorFactory` is used to retrieve the appropriate processor based on the specific `Microsoft.SqlServer.TransactSql.ScriptDom` node type.
*   Key processor categories include:
    *   **Statement Processors**: Handle top-level SQL statements (e.g., `InsertStatementProcessor`, `UpdateStatementProcessor`, `ExecuteStatementProcessor`). They initiate the analysis for a given statement type.
    *   **Query Expression Processors**: Deal with the core query structures like `SELECT` clauses (`QuerySpecificationProcessor`) and set operations like `UNION`/`EXCEPT` (`BinaryQueryExpressionProcessor`). They determine the output columns of a query.
    *   **Table Reference Processors**: Analyze the `FROM` clause elements, identifying data sources. Examples include `NamedTableReferenceProcessor` (for base tables/views/CTEs), `JoinTableReferenceProcessor` (for JOIN clauses), and `QueryDerivedTableProcessor` (for subqueries in the FROM clause).
    *   **Select Element Processors**: Process individual items in the `SELECT` list, such as specific columns (`SelectScalarExpressionProcessor`) or `SELECT *` (`SelectStarExpressionProcessor`). They link output columns back to their sources identified by the Table Reference Processors.
    *   **CTE Scope Processor**: Manages the scope and processing of Common Table Expressions defined in `WITH` clauses.
*   The design relies heavily on interfaces (`IAstProvider`, `IProcessorFactory`, `ILineageGraph`, `IProcessingContext`, etc.) for flexibility and dependency inversion.
*   A `ProcessingContext` object is passed during AST traversal to maintain the current state, including the lineage graph being built, active CTEs, and other relevant information.

## Technology Stack

* .NET 9
* Microsoft.SqlServer.TransactSql.ScriptDom (SQL 2019/2022 grammar)

## How it Works

The analysis follows these general steps:

1.  The CLI tool receives the path to a SQL script file.
2.  The `AstProvider` service parses the SQL script text into an AST using `Microsoft.SqlServer.TransactSql.ScriptDom`. Any parsing errors are collected.
3.  The `LineageAnalyzer` orchestrates the analysis process. It iterates through the statements in the AST.
4.  For each statement (or relevant sub-fragment), the `LineageAnalyzer` uses the `ProcessorFactory` to obtain the correct processor instance.
5.  The selected processor is invoked, passing the specific AST node and the current `ProcessingContext`.
6.  Processors traverse their assigned part of the AST, identify column relationships (e.g., a select list item deriving from a table column), and update the `LineageGraph` within the `ProcessingContext` by adding `ColumnNode`s and `LineageEdge`s. Processors may recursively invoke other processors for nested structures (e.g., a statement processor invoking query expression processors).
7.  Once the AST traversal is complete, a `LineageResult` object is created, containing the final list of nodes, edges, and any parse errors encountered.
8.  The CLI tool serializes the `LineageResult` into JSON format and prints it to the console.

## Quickstart

Single file analysis:

```bash
dotnet run --project ColumnLineageCli -- path/to/query.sql
```

Analyze an SSDT project and export OpenLineage:

```bash
dotnet run --project ColumnLineageCli -- --project path/to/MyDb.sqlproj --openlineage lineage.json
```

Import schema for richer dataset facets when analyzing a single file:

```bash
dotnet run --project ColumnLineageCli -- path/to/query.sql --import-schema schema.json --openlineage ol.json
```

Export schema from a project (to later use with --import-schema):

```bash
dotnet run --project ColumnLineageCli -- --project path/to/MyDb.sqlproj --schema schema.json
```

## Output Format

The tool outputs a JSON object representing the column lineage graph. This object has two main properties: `Nodes` and `Edges`.

*   **`Nodes`**: An array of objects, where each object represents a distinct column identified during analysis. Each node object has the following properties:
    *   `Id` (string): A unique identifier for the column node. This is typically formed as `SourceName.ColumnName` (e.g., "Table1.ColA", "MyCTE.ColB") or just `ColumnName` for columns in the final result set or derived columns without a clear single source alias (e.g., "FinalResultCol").
    *   `Label` (string): The display name of the column, usually the simple column name itself (e.g., "ColA", "ColB", "FinalResultCol").
    *   `Source` (string | null): The name of the immediate source table, view, or CTE that this column belongs to within its current scope. This can be null for columns that are computed or part of the final output without a direct table source in that context.

*   **`Edges`**: An array of objects, where each object represents a directed lineage relationship, indicating data flow from one column node to another. Each edge object has:
    *   `Source` (string): The `Id` of the source column node where the data originates.
    *   `Target` (string): The `Id` of the target column node where the data flows to.

**Example:**

```json
{
  "Nodes": [
    { "Id": "TableA.Col1", "Label": "Col1", "Source": "TableA" },
    { "Id": "TableA.Col2", "Label": "Col2", "Source": "TableA" },
    { "Id": "CTE1.DerivedCol", "Label": "DerivedCol", "Source": "CTE1" },
    { "Id": "Final.OutputCol", "Label": "OutputCol", "Source": null }
  ],
  "Edges": [
    { "Source": "TableA.Col1", "Target": "CTE1.DerivedCol" },
    { "Source": "TableA.Col2", "Target": "CTE1.DerivedCol" },
    { "Source": "CTE1.DerivedCol", "Target": "Final.OutputCol" }
  ]
}
```

## Project Structure

*   **`ColumnLineageCli/`**: Contains the executable console application (`Program.cs`) that takes user input and uses the core library.
*   **`ColumnLineageCore/`**: The main library containing all the lineage analysis logic, interfaces, models, processors, and helpers.
    *   `Interfaces/`: Defines contracts for core components (analyzer, providers, processors, graph).
    *   `Model/`: Defines the core data structures (`ColumnNode`, `LineageEdge`).
    *   `Processors/`: Contains concrete implementations for processing different T-SQL fragments.
    *   `Helpers/`: Utility classes.
        * `SsdtProjectProvider.cs`: Discovers and reads SQL files from SSDT (.sqlproj) with cross-platform path handling.
    *   `Json/`: Classes specifically for structuring the JSON output.
    *   `Export/`: OpenLineage exporter and helpers.
    *   `LineageAnalyzer.cs`: Orchestrates the analysis.
    *   `LineageGraph.cs`: Manages the collection of nodes and edges.
    *   `AstProvider.cs`: Handles parsing SQL into an AST.
    *   `ProcessorFactory.cs`: Creates instances of processors.
*   **`ColumnLineageCore.Tests/`**: Contains xUnit tests for the core library components.

## Building

You can build the entire solution using the standard .NET CLI command from the root directory:

```bash
dotnet build ColumnLineageSolution.sln
```

## Testing

The `ColumnLineageCore.Tests` project contains unit and integration tests. You can run these tests using the .NET CLI from the root directory:

```bash
dotnet test ColumnLineageSolution.sln
```

## OpenLineage Export

Export inputs/outputs with full schema and column lineage facets:

```bash
dotnet run --project ColumnLineageCli -- --project path/to/MyDb.sqlproj \
    --openlineage lineage.json --namespace sqlserver://my-host
```

When analyzing a single file, provide `--import-schema` to enrich input dataset schemas in the output.

## Debugging Complex SQL

The CLI includes switches to help diagnose parser coverage and missing processors:

* `--detailed` – more verbose analysis logs
* `--debug` – print analysis meta and sizes
* `--debug-ast` – dumps a concise AST structure with node types and locations
* `--debug-unhandled` – collects and prints fragment types with no registered processor

Examples:

```bash
# Dump AST for a single script
dotnet run --project ColumnLineageCli -- path/to/query.sql --debug-ast

# Analyze project and list missing processors without failing
dotnet run --project ColumnLineageCli -- --project path/to/MyDb.sqlproj --debug-unhandled
```

If `--debug-unhandled` reports a type like `Microsoft.SqlServer.TransactSql.ScriptDom.QueryParenthesisExpression`, add a processor in `ColumnLineageCore/Processors` and register it in `CreateAndRegisterProcessors` in `Program.cs`.

## Schema Import/Export

You can export schema from a project and later import it when analyzing single files to enrich dataset schemas in OpenLineage.

Schema export format supports either:

* `objects: [...]` (legacy) or
* separate arrays: `tables`, `views`, `functions`, `storedProcedures` (preferred)

When importing, table columns gain types and descriptions in the OpenLineage `schema` facet.

## Processor coverage and known gaps

Implemented processors (as of this repo):

- Statements: InsertStatementProcessor, UpdateStatementProcessor, DeleteStatementProcessor, MergeStatementProcessor, ExecuteStatementProcessor
- Query expressions: QuerySpecificationProcessor, BinaryQueryExpressionProcessor (UNION/INTERSECT/EXCEPT)
- Table references: NamedTableReferenceProcessor, JoinTableReferenceProcessor, QueryDerivedTableProcessor, PivotedTableReferenceProcessor, UnpivotedTableReferenceProcessor, VariableTableReferenceProcessor
- Select elements: SelectScalarExpressionProcessor, SelectStarExpressionProcessor
- CTE handling: CteScopeProcessor (WITH ...)

High‑priority missing processors or areas for full end‑to‑end lineage:

- Query expressions
  - QueryParenthesisExpression (unwrap nested/parenthesized queries)

- Table references (FROM)
  - SchemaObjectFunctionTableReference (table‑valued functions)
  - ApplyTableReference (CROSS APPLY / OUTER APPLY)
  - OpenJsonTableReference (OPENJSON)
  - OpenRowset / OpenQuery / OpenDataSourceTableReference

- Select elements
  - SelectSetVariable (e.g., SELECT @v = col ...)

- Scalar/expressions inside SelectScalarExpression (partial today)
  - FunctionCall (built‑ins, aggregates), OverClause (window functions)
  - CaseExpression (Simple/Searched)
  - CastSpecification / Convert / Try_Convert / Try_Cast
  - BinaryExpression / BooleanBinaryExpression, UnaryExpression
  - ScalarSubquery

- Clauses and semantics
  - GROUP BY / HAVING (aggregate lineage to base columns)
  - DISTINCT (no new lineage but ensure consistent edge semantics)

- DML details
  - OUTPUT clauses for INSERT/UPDATE/DELETE/MERGE
  - MERGE output and per‑branch mappings

- Objects and sources
  - Table‑valued parameters and table variables (@tv)
  - Temporary tables (#temp) — infer/import schemas and propagate
  - Synonyms resolution to underlying objects
  - External tables — treat as named sources with imported schema

How to extend:

1) Implement a processor under `ColumnLineageCore/Processors` for the ScriptDom type you’re handling.
2) Register it in `CreateAndRegisterProcessors` in `ColumnLineageCli/Program.cs`.
3) Use `--debug-unhandled` to discover additional missing fragment types in your workload.

## SSDT Notes

`SsdtProjectProvider` normalizes Windows `\` and macOS/Linux `/` separators and respects `<Build Include=...>` entries. Ensure your `.sqlproj` includes files you want analyzed.
