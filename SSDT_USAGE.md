# SSDT Project Support - Usage Guide

## Overview

The column lineage analyzer now supports analyzing entire SQL Server Database Projects (SSDT) with enhanced features:
- **Flexible Directory Structure**: Automatically discovers SQL objects regardless of folder organization
- **Dynamic Dependency Resolution**: Tracks function calls, stored procedure executions, and cross-object references
- **OpenLineage Export**: Exports lineage data in OpenLineage format for integration with data catalogs and governance tools
- **Enhanced Object Discovery**: Uses SQL DOM parsing for accurate dependency extraction

## Usage

### Analyze Single SQL File (Original)
```bash
# Existing functionality - single file analysis
dotnet run -- path/to/your/script.sql
```

### Analyze SSDT Project (Enhanced)
```bash
# Basic project analysis
dotnet run -- --project path/to/your/project.sqlproj
dotnet run -- --ssdt path/to/your/project/directory

# With OpenLineage export
dotnet run -- --project path/to/project --openlineage lineage_output.json

# Detailed analysis with custom namespace
dotnet run -- --project path/to/project --openlineage lineage.json --namespace "sqlserver://prod-server" --detailed
```

## Command Line Options

| Option | Description | Example |
|--------|-------------|---------|
| `--project <path>` | Analyze SSDT project | `--project ./MyDatabase.sqlproj` |
| `--ssdt <path>` | Alternative syntax for project analysis | `--ssdt ./MyDatabase/` |
| `--openlineage <file>` | Export to OpenLineage format | `--openlineage output.json` |
| `--namespace <uri>` | Set namespace for OpenLineage | `--namespace "sqlserver://prod"` |
| `--detailed` | Show detailed object analysis | `--detailed` |

## Enhanced Directory Structure Support

The analyzer now automatically discovers SQL objects in **any** directory structure:

```
YourProject/
├── Database.sqlproj              # Optional - will be parsed if present
├── src/
│   ├── schemas/
│   │   ├── sales_schema.sql     # Schema definitions anywhere
│   │   └── hr_schema.sql
│   ├── customer_tables/
│   │   ├── customers.sql        # Tables in any folder
│   │   ├── orders.sql
│   │   └── order_items.sql
│   ├── reporting/
│   │   ├── customer_view.sql    # Views anywhere
│   │   └── sales_summary.sql
│   └── automation/
│       ├── data_import.sql      # Stored procedures
│       └── cleanup_job.sql
├── functions/
│   ├── date_utils.sql           # Functions
│   └── string_helpers.sql
└── misc/
    └── utility_objects.sql      # Mixed object types
```

## Dynamic Dependency Resolution

### Function Call Tracking
```sql
-- In stored procedure: sp_ProcessOrder.sql
EXEC dbo.sp_ValidateCustomer @CustomerID
SELECT dbo.fn_CalculateDiscount(@ProductID, @Quantity)

-- Automatically detects dependencies:
-- sp_ProcessOrder -> sp_ValidateCustomer
-- sp_ProcessOrder -> fn_CalculateDiscount
```

### Cross-Schema References
```sql
-- In view: Sales.vCustomerOrders.sql
SELECT c.CustomerID, c.Name
FROM Sales.Customer c
JOIN Production.Orders o ON c.CustomerID = o.CustomerID

-- Detects dependencies:
-- Sales.vCustomerOrders -> Sales.Customer
-- Sales.vCustomerOrders -> Production.Orders
```

### Dynamic SQL Detection (Basic)
```sql
-- Detects table references in dynamic SQL strings
DECLARE @sql NVARCHAR(MAX) = 'SELECT * FROM Sales.Customer WHERE CustomerID = @id'
```

## OpenLineage Export

### Output Format
The OpenLineage export creates a comprehensive lineage document:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2025-08-25T10:30:00Z",
  "run": {
    "runId": "uuid-here",
    "facets": {
      "nominalTime": {
        "nominalStartTime": "2025-08-25T10:30:00Z"
      }
    }
  },
  "job": {
    "namespace": "sqlserver://localhost",
    "name": "MyDatabase",
    "facets": {
      "documentation": {
        "description": "SSDT Project: MyDatabase"
      },
      "sourceCode": {
        "language": "sql",
        "source": "SSDT Project Analysis"
      }
    }
  },
  "inputs": [
    {
      "namespace": "sqlserver://localhost",
      "name": "[Sales].[Customer]",
      "facets": {
        "schema": {
          "fields": [
            {
              "name": "CustomerID",
              "type": "int",
              "description": ""
            },
            {
              "name": "CustomerName",
              "type": "nvarchar(100)",
              "description": ""
            }
          ]
        },
        "documentation": {
          "description": "Table: [Sales].[Customer]",
          "filePath": "/path/to/Customer.sql"
        },
        "dataSource": {
          "name": "SSDT Project",
          "uri": "/path/to/Customer.sql"
        }
      }
    }
  ],
  "outputs": [
    {
      "namespace": "sqlserver://localhost",
      "name": "[Sales].[vCustomerOrders]",
      "facets": {
        "dependencies": ["Sales.Customer", "Sales.Orders"]
      }
    }
  ]
}
```

### Integration with Data Catalogs

The OpenLineage format can be consumed by:
- **Apache Atlas**
- **DataHub**
- **Amundsen**
- **Custom lineage tools**

Example integration:
```bash
# Export lineage
dotnet run -- --project ./MyDatabase --openlineage lineage.json

# Send to DataHub
curl -X POST http://datahub:8080/openlineage \
  -H "Content-Type: application/json" \
  -d @lineage.json
```

## Enhanced Features

### 1. Intelligent Object Detection
- **SQL DOM Parsing**: Uses Microsoft SQL Server parser for accurate syntax analysis
- **Content-Based Detection**: Analyzes SQL content when directory structure is unclear
- **Multi-Pattern Support**: Handles various SSDT project layouts

### 2. Comprehensive Dependency Tracking
- **Static Dependencies**: FROM, JOIN, INSERT INTO, UPDATE, DELETE
- **Dynamic Dependencies**: EXEC statements, function calls
- **CTE References**: Common Table Expression dependencies
- **Cross-Schema References**: Fully qualified object names

### 3. Rich Metadata Extraction
- **Column Information**: Names, data types, constraints
- **Parameter Details**: Stored procedure and function parameters
- **Comments**: Inline and block comments
- **File Information**: Paths, modification dates, sizes

### 4. Error Handling & Reporting
```bash
# Example output with errors
Discovered 45 SQL objects in project:
  Table: 12 objects
  View: 8 objects
  StoredProcedure: 15 objects
  Function: 10 objects

Objects with Parsing Errors (3):
- [dbo].[ComplexView]: 2 errors
- [Sales].[LegacyProcedure]: 1 errors
- [Production].[InvalidFunction]: 5 errors

Project Analysis Summary:
  Total Nodes: 156
  Total Edges: 89
  Objects with Errors: 3
```

## Performance & Scalability

### Processing Order
1. **Schemas** → 2. **Tables** → 3. **Views** → 4. **Procedures** → 5. **Functions**

### Memory Usage
- Efficient SQL DOM parsing
- Streaming file processing
- Configurable memory limits

### Large Project Support
- Progress reporting for 100+ objects
- Parallel processing where safe
- Incremental analysis support

## Limitations & Future Enhancements

### Current Limitations
1. **Dynamic SQL**: Limited detection of runtime-generated SQL
2. **External Dependencies**: No resolution of linked server or external database references
3. **Conditional Logic**: Complex conditional dependencies in procedures
4. **Temporary Objects**: Limited tracking of temp tables and variables

### Planned Enhancements
1. **Advanced Dynamic SQL Analysis**
2. **Cross-Database Lineage**
3. **Real-time Change Detection**
4. **Visual Lineage Diagrams**
5. **Impact Analysis Reports**

## Example Workflows

### 1. CI/CD Integration
```yaml
# Azure DevOps Pipeline
- task: DotNetCoreCLI@2
  displayName: 'Analyze Database Lineage'
  inputs:
    command: 'run'
    arguments: '--project $(Build.SourcesDirectory)/Database --openlineage $(Build.ArtifactStagingDirectory)/lineage.json'
```

### 2. Documentation Generation
```bash
# Generate comprehensive project documentation
dotnet run -- --project ./MyDatabase --detailed > database_analysis.txt
dotnet run -- --project ./MyDatabase --openlineage lineage.json --namespace "sqlserver://$(SERVER_NAME)"
```

### 3. Impact Analysis
```bash
# Before making changes, understand dependencies
dotnet run -- --project ./MyDatabase --detailed | grep "CustomerTable"
```
