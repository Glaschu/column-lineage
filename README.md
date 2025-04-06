# SQL Column-Level Lineage Analyzer

## Overview

This tool analyzes T-SQL scripts to determine column-level data lineage. It parses SQL code using `Microsoft.SqlServer.TransactSql.ScriptDom`, builds an Abstract Syntax Tree (AST), and traverses it to identify how data flows between columns across different statements, tables, views, and Common Table Expressions (CTEs). The final output is a JSON representation of the lineage graph.

## Core Features

*   Parses T-SQL scripts into Abstract Syntax Trees (ASTs).
*   Traverses ASTs to identify column dependencies and data flow.
*   Builds a directed graph representing column-to-column lineage.
*   Handles common SQL complexities like CTEs, table/column aliases, JOINs, and basic DML statements (SELECT, INSERT, UPDATE, EXECUTE).
*   Supports resolving external view definitions (currently via `FileSystemViewDefinitionProvider` looking in a `./views` subdirectory relative to the execution path).
*   Outputs the calculated lineage graph as a structured JSON object.

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

*   .NET 8
*   Microsoft.SqlServer.TransactSql.ScriptDom (Version 170.x)

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

## Usage

1.  Navigate to the command-line interface project directory:
    ```bash
    cd ColumnLineageCli
    ```
2.  Build the solution (optional, `dotnet run` handles it):
    ```bash
    dotnet build ../ColumnLineageSolution.sln
    ```
3.  Run the tool, providing the path to the SQL file you want to analyze:
    ```bash
    dotnet run -- <path_to_your_sql_file.sql>
    ```
    For example:
    ```bash
    dotnet run -- ../../my_scripts/complex_query.sql
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
    *   `Json/`: Classes specifically for structuring the JSON output.
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
