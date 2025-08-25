﻿using System;
using System.IO;
using System.Linq; // Added for Any()
using System.Text; // Added for StringBuilder
using System.Text.Json; // Added for JSON serialization
using ColumnLineageCore;
using ColumnLineageCore.Interfaces; // For IAstProvider, IProcessorFactory, IProjectProvider
using ColumnLineageCore.Interfaces.Processors; // For ICteScopeProcessor
using ColumnLineageCore.Processors; // For concrete processors
using ColumnLineageCore.Helpers; // Added for FileSystemViewDefinitionProvider
using ColumnLineageCore.Export; // For OpenLineage export
using Microsoft.SqlServer.TransactSql.ScriptDom; // For TSqlFragment types

namespace ColumnLineageCli
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("  ColumnLineageCli <path_to_sql_file>           # Analyze single SQL file");
                Console.WriteLine("  ColumnLineageCli --project <project_path>    # Analyze SSDT project");
                Console.WriteLine("  ColumnLineageCli --ssdt <project_path>       # Analyze SSDT project");
                Console.WriteLine("  ColumnLineageCli --project <project_path> --openlineage <output_file>  # Export to OpenLineage");
                Console.WriteLine("  ColumnLineageCli --project <project_path> --schema <schema_file>        # Export schema JSON");
                Console.WriteLine("  ColumnLineageCli --project <project_path> --import-schema <schema_file> # Import schema for enhanced analysis");
                Console.WriteLine("");
                Console.WriteLine("Options:");
                Console.WriteLine("  --openlineage <file>    Export results in OpenLineage format");
                Console.WriteLine("  --schema <file>         Export schema JSON for tables, views, and functions");
                Console.WriteLine("  --import-schema <file>  Import existing schema JSON to enhance lineage analysis");
                Console.WriteLine("  --detailed              Show detailed object analysis");
                Console.WriteLine("  --namespace <ns>        Set namespace for OpenLineage export (default: sqlserver://localhost)");
                Console.WriteLine("  --debug                 Enable debug mode with detailed processor information");
                Console.WriteLine("  --debug-ast             Show AST structure for debugging parser issues");
                Console.WriteLine("  --debug-unhandled       Show unhandled AST node types for processor development");
                return;
            }

            // Check for project mode
            if (args.Length >= 2 && (args[0] == "--project" || args[0] == "--ssdt"))
            {
                string projectPath = args[1];
                
                // Check for additional options
                string? openLineageFile = null;
                string? schemaFile = null;
                string? importSchemaFile = null;
                string? namespaceValue = null;
                bool detailed = false;
                bool debug = false;
                bool debugAst = false;
                bool debugUnhandled = false;

                for (int i = 2; i < args.Length; i++)
                {
                    if (args[i] == "--openlineage" && i + 1 < args.Length)
                    {
                        openLineageFile = args[i + 1];
                        i++; // skip the next argument
                    }
                    else if (args[i] == "--schema" && i + 1 < args.Length)
                    {
                        schemaFile = args[i + 1];
                        i++; // skip the next argument
                    }
                    else if (args[i] == "--import-schema" && i + 1 < args.Length)
                    {
                        importSchemaFile = args[i + 1];
                        i++; // skip the next argument
                    }
                    else if (args[i] == "--namespace" && i + 1 < args.Length)
                    {
                        namespaceValue = args[i + 1];
                        i++; // skip the next argument
                    }
                    else if (args[i] == "--detailed")
                    {
                        detailed = true;
                    }
                    else if (args[i] == "--debug")
                    {
                        debug = true;
                    }
                    else if (args[i] == "--debug-ast")
                    {
                        debugAst = true;
                    }
                    else if (args[i] == "--debug-unhandled")
                    {
                        debugUnhandled = true;
                    }
                }

                AnalyzeSsdtProject(projectPath, openLineageFile, schemaFile, importSchemaFile, namespaceValue, detailed);
                return;
            }

            // Single file mode (existing functionality)
            string filePath = args[0];
            if (!File.Exists(filePath))
            {
                // Check if it's a directory (might be an SSDT project)
                if (Directory.Exists(filePath))
                {
                    Console.WriteLine($"Directory detected: '{filePath}'");
                    Console.WriteLine("Use --project flag for SSDT project analysis:");
                    Console.WriteLine($"  ColumnLineageCli --project \"{filePath}\"");
                    return;
                }
                Console.WriteLine($"Error: File not found at '{filePath}'");
                return;
            }

            // Parse additional arguments for single file mode
            string? singleFileOpenLineageFile = null;
            string? singleFileSchemaFile = null;
            string? singleFileImportSchemaFile = null;
            string singleFileNamespaceValue = "sqlserver://localhost";
            bool singleFileDetailed = false;
            bool singleFileDebug = false;
            bool singleFileDebugAst = false;
            bool singleFileDebugUnhandled = false;

            for (int i = 1; i < args.Length; i++)
            {
                if (args[i] == "--openlineage" && i + 1 < args.Length)
                {
                    singleFileOpenLineageFile = args[i + 1];
                    i++; // skip the next argument
                }
                else if (args[i] == "--schema" && i + 1 < args.Length)
                {
                    singleFileSchemaFile = args[i + 1];
                    i++; // skip the next argument
                }
                else if (args[i] == "--import-schema" && i + 1 < args.Length)
                {
                    singleFileImportSchemaFile = args[i + 1];
                    i++; // skip the next argument
                }
                else if (args[i] == "--namespace" && i + 1 < args.Length)
                {
                    singleFileNamespaceValue = args[i + 1];
                    i++; // skip the next argument
                }
                else if (args[i] == "--detailed")
                {
                    singleFileDetailed = true;
                }
                else if (args[i] == "--debug")
                {
                    singleFileDebug = true;
                }
                else if (args[i] == "--debug-ast")
                {
                    singleFileDebugAst = true;
                }
                else if (args[i] == "--debug-unhandled")
                {
                    singleFileDebugUnhandled = true;
                }
            }

            AnalyzeSingleFile(filePath, singleFileOpenLineageFile, singleFileSchemaFile, singleFileImportSchemaFile, singleFileNamespaceValue, singleFileDetailed, singleFileDebug, singleFileDebugAst, singleFileDebugUnhandled);
        }

        static void AnalyzeSingleFile(string filePath, string? openLineageFile = null, string? schemaFile = null, string? importSchemaFile = null, string namespaceValue = "sqlserver://localhost", bool detailed = false, bool debug = false, bool debugAst = false, bool debugUnhandled = false)
        {
            try
            {
                // --- Load existing schema if provided ---
                ImportedSchemaInfo? importedSchema = null;
                if (!string.IsNullOrEmpty(importSchemaFile))
                {
                    try
                    {
                        importedSchema = LoadSchemaFromFile(importSchemaFile);
                        Console.WriteLine($"Loaded schema from: {importSchemaFile}");
                        Console.WriteLine($"  Tables: {importedSchema.Tables.Count}, Views: {importedSchema.Views.Count}");
                        Console.WriteLine($"  Functions: {importedSchema.Functions.Count}, Procedures: {importedSchema.StoredProcedures.Count}");
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"Warning: Failed to load schema file '{importSchemaFile}': {ex.Message}");
                        Console.ResetColor();
                    }
                }

                string sqlScript = File.ReadAllText(filePath);

                // --- Setup Dependencies for LineageAnalyzer ---
                // 1. AST Provider: Parses the SQL script
                IAstProvider astProvider = new AstProvider();

                // 2. Processor Factory: Creates processors for specific SQL fragments
                // Pass astProvider to register processors that need it
                IProcessorFactory processorFactory = CreateAndRegisterProcessors(astProvider);

                // 3. CTE Scope Processor: Handles WITH clauses and dependencies
                ICteScopeProcessor cteScopeProcessor = new CteScopeProcessor();

                // 4. View Definition Provider: Loads view definitions (using default './views' directory)
                IViewDefinitionProvider viewProvider = new FileSystemViewDefinitionProvider(); // Added

                // --- Instantiate the Analyzer ---
                // The analyzer orchestrates the process using the dependencies
                var analyzer = new LineageAnalyzer(astProvider, processorFactory, cteScopeProcessor, viewProvider); // Added viewProvider

                // --- Debug Mode: Show AST structure if requested ---
                if (debugAst)
                {
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine("\n=== AST STRUCTURE DEBUG ===");
                    Console.ResetColor();
                    ShowAstStructure(sqlScript, astProvider);
                }

                // --- Check if we need to extract SQL from CREATE statements ---
                string analyzableSql = ExtractAnalyzableSqlFromScript(sqlScript);
                string scriptToAnalyze = string.IsNullOrEmpty(analyzableSql) ? sqlScript : analyzableSql;

                // --- Perform Analysis ---
                if (debug)
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("\n=== DEBUG MODE ENABLED ===");
                    Console.ResetColor();
                    Console.WriteLine($"Debug flags: debug={debug}, debugAst={debugAst}, debugUnhandled={debugUnhandled}");
                    Console.WriteLine($"Script length: {scriptToAnalyze.Length} characters");
                }

                Console.WriteLine($"Analyzing script: {filePath}...");
                if (!string.IsNullOrEmpty(analyzableSql))
                {
                    Console.WriteLine("Extracted analyzable SQL from CREATE statement(s):");
                    Console.WriteLine($"  SQL Length: {analyzableSql.Length} characters");
                }
                var lineageResult = analyzer.Analyze(scriptToAnalyze); // Use the new analyzer

                // --- Output the result as JSON ---
                // Check for and display any SQL parsing errors first
                if (lineageResult.Errors != null && lineageResult.Errors.Any())
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("\nSQL Parse Errors Encountered:");
                    Console.ResetColor();
                    foreach (var error in lineageResult.Errors)
                    {
                        Console.WriteLine($"- Line {error.Line}, Col {error.Column}: {error.Message}");
                    }
                    Console.WriteLine("\nLineage analysis might be incomplete due to errors.");
                }

                // Serialize the graph (nodes and edges) to JSON
                string jsonOutput = lineageResult.ToJson(indented: true);
                Console.WriteLine("\n--- Lineage Result (JSON) ---");
                Console.WriteLine(jsonOutput);

                // --- Export to OpenLineage format if requested ---
                if (!string.IsNullOrEmpty(openLineageFile))
                {
                    try
                    {
                        var exporter = new OpenLineageExporter();
                        var projectName = Path.GetFileNameWithoutExtension(filePath);
                        
                        // Create a minimal SqlObjectDefinition for the single file
                        var sqlObjectDef = new SqlObjectDefinition
                        {
                            Name = projectName,
                            Type = SqlObjectType.View, // Using View as closest match for SQL script
                            SqlContent = sqlScript,
                            FilePath = filePath
                        };

                        // Create a ProcessedObject
                        var processedObject = new ProcessedObject
                        {
                            SqlObject = sqlObjectDef,
                            LineageResult = lineageResult
                        };

                        // Create a ProjectLineageResult containing this single object
                        var projectResult = new ProjectLineageResult();
                        projectResult.ProcessedObjects.Add(processedObject);
                        projectResult.CombinedLineage = lineageResult;

                        // Create input datasets for tables referenced in the lineage
                        var referencedTables = lineageResult.Nodes
                            .Where(n => !string.IsNullOrEmpty(n.SourceName))
                            .Select(n => n.SourceName)
                            .Distinct()
                            .ToList();

                        foreach (var tableName in referencedTables)
                        {
                            var tableObject = new SqlObjectDefinition
                            {
                                Name = tableName!,
                                Type = SqlObjectType.Table,
                                Schema = "dbo", // default schema
                                DatasetNamespace = namespaceValue
                            };

                            // If we have imported schema, populate the column definitions
                            if (importedSchema != null)
                            {
                                var schemaTable = importedSchema.Tables.Values.FirstOrDefault(t => 
                                    t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));
                                
                                if (schemaTable != null)
                                {
                                    Console.WriteLine($"Found schema for table: {tableName} ({schemaTable.Columns.Count} columns)");
                                    tableObject.Schema = schemaTable.Schema;
                                    tableObject.ColumnDefinitions = schemaTable.Columns.Select(c => new SqlColumnDefinition
                                    {
                                        Name = c.Name,
                                        DataType = c.DataType,
                                        Description = c.Description ?? $"Column from {tableName}"
                                    }).ToList();
                                }
                                else
                                {
                                    Console.WriteLine($"No schema found for table: {tableName} (using inferred schema)");
                                }
                            }

                            projectResult.DiscoveredObjects.Add(tableObject);
                        }

                        // Add the output object (the query result) as a view
                        sqlObjectDef.DatasetNamespace = namespaceValue;
                        projectResult.DiscoveredObjects.Add(sqlObjectDef);

                        var openLineageDoc = exporter.ExportProject(projectResult, projectName, namespaceValue);
                        var openLineageJson = exporter.ExportToJson(openLineageDoc, indented: true);
                        
                        File.WriteAllText(openLineageFile, openLineageJson);
                        Console.WriteLine($"\nOpenLineage export saved to: {openLineageFile}");
                        Console.WriteLine($"Datasets: {openLineageDoc.Inputs.Count + openLineageDoc.Outputs.Count}");
                        Console.WriteLine($"Namespace: {namespaceValue}");
                    }
                    catch (Exception exportEx)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Error exporting OpenLineage: {exportEx.Message}");
                        Console.ResetColor();
                    }
                }

                // --- Export schema if requested ---
                if (!string.IsNullOrEmpty(schemaFile))
                {
                    try
                    {
                        // For single file, create a minimal schema export
                        var schemaData = new
                        {
                            tables = new List<object>(),
                            views = new List<object>(),
                            functions = new List<object>(),
                            storedProcedures = new List<object>(),
                            exportDate = DateTime.UtcNow.ToString("O")
                        };

                        string schemaJson = JsonSerializer.Serialize(schemaData, new JsonSerializerOptions { WriteIndented = true });
                        File.WriteAllText(schemaFile, schemaJson);
                        Console.WriteLine($"\nSchema export saved to: {schemaFile}");
                    }
                    catch (Exception schemaEx)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine($"Error exporting schema: {schemaEx.Message}");
                        Console.ResetColor();
                    }
                }

            }
            catch (Exception ex)
            {
                // Catch any unexpected errors during analysis
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\nAn error occurred during analysis: {ex.Message}");
                Console.ResetColor();
                Console.WriteLine("--- Stack Trace ---");
                Console.WriteLine(ex.StackTrace);
            }
        }

        static void AnalyzeSsdtProject(string projectPath, string? openLineageFile = null, string? schemaFile = null, string? importSchemaFile = null, string? namespaceValue = null, bool detailed = false)
        {
            try
            {
                Console.WriteLine($"Analyzing SSDT project: {projectPath}...");
                
                // --- Load existing schema if provided ---
                ImportedSchemaInfo? importedSchema = null;
                if (!string.IsNullOrEmpty(importSchemaFile))
                {
                    try
                    {
                        importedSchema = LoadSchemaFromFile(importSchemaFile);
                        Console.WriteLine($"Loaded schema from: {importSchemaFile}");
                        Console.WriteLine($"  Tables: {importedSchema.Tables.Count}, Views: {importedSchema.Views.Count}");
                        Console.WriteLine($"  Functions: {importedSchema.Functions.Count}, Procedures: {importedSchema.StoredProcedures.Count}");
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"Warning: Failed to load schema file '{importSchemaFile}': {ex.Message}");
                        Console.ResetColor();
                    }
                }
                
                // --- Setup Dependencies for Project Analysis ---
                IAstProvider astProvider = new AstProvider();
                IProcessorFactory processorFactory = CreateAndRegisterProcessors(astProvider);
                ICteScopeProcessor cteScopeProcessor = new CteScopeProcessor();
                
                // --- Create Enhanced Project Provider if schema is imported ---
                IProjectProvider projectProvider;
                if (importedSchema != null)
                {
                    projectProvider = new EnhancedSsdtProjectProvider(new SsdtProjectProvider(), importedSchema);
                    Console.WriteLine("Using enhanced project provider with imported schema information");
                }
                else
                {
                    projectProvider = new SsdtProjectProvider();
                }

                // --- Instantiate the Project Analyzer ---
                var projectAnalyzer = new ProjectLineageAnalyzer(
                    astProvider, 
                    processorFactory, 
                    cteScopeProcessor, 
                    projectProvider);

                // --- Perform Project Analysis ---
                var projectResult = projectAnalyzer.AnalyzeProject(projectPath);

                // --- Output Results ---
                if (projectResult.ProjectErrors.Any())
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("\nProject-Level Errors:");
                    Console.ResetColor();
                    foreach (var error in projectResult.ProjectErrors)
                    {
                        Console.WriteLine($"- {error}");
                    }
                }

                // Check for parsing errors across all objects
                var objectsWithErrors = projectResult.ProcessedObjects
                    .Where(p => p.LineageResult.Errors.Any())
                    .ToList();

                if (objectsWithErrors.Any())
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"\nObjects with Parsing Errors ({objectsWithErrors.Count}):");
                    Console.ResetColor();
                    foreach (var obj in objectsWithErrors)
                    {
                        Console.WriteLine($"- {obj.SqlObject.FullName}: {obj.LineageResult.Errors.Count} errors");
                    }
                }

                // Serialize the combined lineage graph to JSON
                string jsonOutput = projectResult.CombinedLineage.ToJson(indented: true);
                Console.WriteLine("\n--- Combined Project Lineage (JSON) ---");
                Console.WriteLine(jsonOutput);

                // Export to OpenLineage if requested
                if (!string.IsNullOrEmpty(openLineageFile))
                {
                    try
                    {
                        var exporter = new OpenLineageExporter();
                        var projectName = Path.GetFileNameWithoutExtension(projectPath);
                        var exportNamespace = namespaceValue ?? "sqlserver://localhost";
                        
                        var openLineageDoc = exporter.ExportProject(projectResult, projectName, exportNamespace);
                        var openLineageJson = exporter.ExportToJson(openLineageDoc, indented: true);
                        
                        File.WriteAllText(openLineageFile, openLineageJson);
                        Console.WriteLine($"\n--- OpenLineage Export ---");
                        Console.WriteLine($"Exported to: {openLineageFile}");
                        Console.WriteLine($"Datasets: {openLineageDoc.Inputs.Count + openLineageDoc.Outputs.Count}");
                        Console.WriteLine($"Namespace: {exportNamespace}");
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"\nWarning: Failed to export OpenLineage: {ex.Message}");
                        Console.ResetColor();
                    }
                }

                // Export schema JSON if requested
                if (!string.IsNullOrEmpty(schemaFile))
                {
                    try
                    {
                        var schemaJson = ExportSchemaToJson(projectResult);
                        File.WriteAllText(schemaFile, schemaJson);
                        Console.WriteLine($"\n--- Schema Export ---");
                        Console.WriteLine($"Schema exported to: {schemaFile}");
                        Console.WriteLine($"Tables: {projectResult.DiscoveredObjects.Count(o => o.Type == SqlObjectType.Table)}");
                        Console.WriteLine($"Views: {projectResult.DiscoveredObjects.Count(o => o.Type == SqlObjectType.View)}");
                        Console.WriteLine($"Functions: {projectResult.DiscoveredObjects.Count(o => o.Type == SqlObjectType.Function)}");
                        Console.WriteLine($"Stored Procedures: {projectResult.DiscoveredObjects.Count(o => o.Type == SqlObjectType.StoredProcedure)}");
                    }
                    catch (Exception ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine($"\nWarning: Failed to export schema: {ex.Message}");
                        Console.ResetColor();
                    }
                }

                // Optional: Generate individual object reports
                if (detailed)
                {
                    Console.WriteLine("\n--- Individual Object Analysis ---");
                    foreach (var processedObj in projectResult.ProcessedObjects)
                    {
                        Console.WriteLine($"\n{processedObj.SqlObject.Type}: {processedObj.SqlObject.FullName}");
                        Console.WriteLine($"  File: {processedObj.SqlObject.FilePath}");
                        Console.WriteLine($"  Columns: {processedObj.SqlObject.Columns.Count}");
                        Console.WriteLine($"  Dependencies: {processedObj.SqlObject.Dependencies.Count}");
                        Console.WriteLine($"  Lineage Nodes: {processedObj.LineageResult.Nodes.Count}");
                        Console.WriteLine($"  Lineage Edges: {processedObj.LineageResult.Edges.Count}");
                        
                        if (processedObj.SqlObject.Dependencies.Any())
                        {
                            Console.WriteLine($"  Depends on: {string.Join(", ", processedObj.SqlObject.Dependencies)}");
                        }
                        
                        if (processedObj.LineageResult.Errors.Any())
                        {
                            Console.WriteLine($"  Errors: {processedObj.LineageResult.Errors.Count}");
                        }
                    }
                }

                // Optional: Generate individual object reports (future enhancement)
                // Could add --detailed flag support here

            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"\nAn error occurred during project analysis: {ex.Message}");
                Console.ResetColor();
                Console.WriteLine("--- Stack Trace ---");
                Console.WriteLine(ex.StackTrace);
            }
        }

        // Helper method to create and configure the ProcessorFactory
        // Added IAstProvider parameter for processors needing it (like ExecuteStatementProcessor)
        private static IProcessorFactory CreateAndRegisterProcessors(IAstProvider astProvider)
        {
            var factory = new ProcessorFactory();

            Console.WriteLine("Registering processors...");

            // Register Table Reference Processors
            factory.RegisterProcessor<NamedTableReference, NamedTableReferenceProcessor>();
            factory.RegisterProcessor<JoinTableReference, JoinTableReferenceProcessor>();
            factory.RegisterProcessor<QueryDerivedTable, QueryDerivedTableProcessor>();
            factory.RegisterProcessor<PivotedTableReference, PivotedTableReferenceProcessor>();
            factory.RegisterProcessor<UnpivotedTableReference, UnpivotedTableReferenceProcessor>();
            factory.RegisterProcessor<VariableTableReference, VariableTableReferenceProcessor>();

            // Register Select Element Processors
            factory.RegisterProcessor<SelectScalarExpression, SelectScalarExpressionProcessor>();
            factory.RegisterProcessor<SelectStarExpression, SelectStarExpressionProcessor>();

            // Register Query Expression Processors
            factory.RegisterProcessor<QuerySpecification, QuerySpecificationProcessor>();
            factory.RegisterProcessor<BinaryQueryExpression, BinaryQueryExpressionProcessor>();

            // Register Statement Processors
            // Example: factory.RegisterProcessor<SelectStatement, SelectStatementProcessor>(); // We use LineageAnalyzer's ProcessAnyQueryExpression instead
            factory.RegisterProcessor<InsertStatement, InsertStatementProcessor>();
            factory.RegisterProcessor<UpdateStatement, UpdateStatementProcessor>();
            factory.RegisterProcessor<MergeStatement, MergeStatementProcessor>();
            factory.RegisterProcessor<DeleteStatement, DeleteStatementProcessor>();
            // Register ExecuteStatementProcessor (requires IAstProvider)
            factory.RegisterProcessorInstance<ExecuteStatement>(new ExecuteStatementProcessor(astProvider));

            Console.WriteLine("Processor registration complete.");
            return factory;
        }

        /// <summary>
        /// Extracts analyzable SQL from CREATE statements (procedures, functions, views)
        /// </summary>
        static string ExtractAnalyzableSqlFromScript(string sqlScript)
        {
            try
            {
                var parser = new TSql160Parser(true);
                IList<ParseError> errors;
                var fragment = parser.Parse(new StringReader(sqlScript), out errors);
                
                if (fragment == null)
                {
                    return string.Empty;
                }
                
                if (errors.Count > 0)
                {
                    return string.Empty;
                }

                var extractor = new SingleFileAnalyzableSqlExtractor();
                fragment.Accept(extractor);
                
                return extractor.ExtractedSql;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[Program] Error extracting SQL: {ex.Message}");
                return string.Empty;
            }
        }

        /// <summary>
        /// Exports the schema information from the project analysis to JSON format.
        /// This includes tables, views, functions, and stored procedures with their column definitions.
        /// </summary>
        /// <param name="projectResult">The project lineage analysis result</param>
        /// <returns>JSON string containing the schema information</returns>
        static string ExportSchemaToJson(ProjectLineageResult projectResult)
        {
            var schema = new
            {
                exportedAt = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ"),
                version = "1.0",
                objects = projectResult.DiscoveredObjects
                    .Where(obj => obj.Type != SqlObjectType.Schema) // Exclude schema objects
                    .OrderBy(obj => obj.Type)
                    .ThenBy(obj => obj.Schema)
                    .ThenBy(obj => obj.Name)
                    .Select(obj => new
                    {
                        name = obj.Name,
                        schema = obj.Schema,
                        fullName = obj.FullName,
                        type = obj.Type.ToString().ToLowerInvariant(),
                        filePath = obj.FilePath,
                        columns = obj.ColumnDefinitions?.Any() == true 
                            ? obj.ColumnDefinitions.Select(col => new
                            {
                                name = col.Name,
                                dataType = col.DataType,
                                description = col.Description,
                                tags = col.Tags.Any() ? (object?)col.Tags : null
                            }).ToArray()
                            : obj.Columns.Select(colName => new
                            {
                                name = colName,
                                dataType = "unknown",
                                description = "",
                                tags = (object?)null
                            }).ToArray(),
                        dependencies = obj.Dependencies.Any() ? obj.Dependencies.ToArray() : null,
                        parameters = obj.Parameters.Any() ? obj.Parameters.ToArray() : null,
                        metadata = obj.Metadata.Any() ? obj.Metadata : null,
                        datasetNamespace = !string.IsNullOrEmpty(obj.DatasetNamespace) ? obj.DatasetNamespace : null
                    })
                    .ToArray(),
                summary = new
                {
                    totalObjects = projectResult.DiscoveredObjects.Count(obj => obj.Type != SqlObjectType.Schema),
                    tables = projectResult.DiscoveredObjects.Count(obj => obj.Type == SqlObjectType.Table),
                    views = projectResult.DiscoveredObjects.Count(obj => obj.Type == SqlObjectType.View),
                    storedProcedures = projectResult.DiscoveredObjects.Count(obj => obj.Type == SqlObjectType.StoredProcedure),
                    functions = projectResult.DiscoveredObjects.Count(obj => obj.Type == SqlObjectType.Function),
                    schemasIncluded = projectResult.DiscoveredObjects
                        .Where(obj => obj.Type != SqlObjectType.Schema)
                        .Select(obj => obj.Schema)
                        .Distinct()
                        .OrderBy(s => s)
                        .ToArray()
                }
            };

            return System.Text.Json.JsonSerializer.Serialize(schema, new System.Text.Json.JsonSerializerOptions
            {
                WriteIndented = true,
                PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase,
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
            });
        }

        /// <summary>
        /// Represents imported schema information from a previously exported schema JSON file.
        /// </summary>
        public class ImportedSchemaInfo
        {
            public Dictionary<string, ImportedObjectInfo> Tables { get; set; } = new();
            public Dictionary<string, ImportedObjectInfo> Views { get; set; } = new();
            public Dictionary<string, ImportedObjectInfo> Functions { get; set; } = new();
            public Dictionary<string, ImportedObjectInfo> StoredProcedures { get; set; } = new();
        }

        /// <summary>
        /// Represents an imported object with enhanced metadata.
        /// </summary>
        public class ImportedObjectInfo
        {
            public string Name { get; set; } = string.Empty;
            public string Schema { get; set; } = string.Empty;
            public string FullName { get; set; } = string.Empty;
            public List<ImportedColumnInfo> Columns { get; set; } = new();
            public List<string> Dependencies { get; set; } = new();
            public Dictionary<string, object> Metadata { get; set; } = new();
        }

        /// <summary>
        /// Represents an imported column with type information.
        /// </summary>
        public class ImportedColumnInfo
        {
            public string Name { get; set; } = string.Empty;
            public string DataType { get; set; } = string.Empty;
            public string Description { get; set; } = string.Empty;
            public Dictionary<string, object>? Tags { get; set; }
        }

        /// <summary>
        /// Loads schema information from a previously exported schema JSON file.
        /// </summary>
        static ImportedSchemaInfo LoadSchemaFromFile(string schemaFilePath)
        {
            if (!File.Exists(schemaFilePath))
            {
                throw new FileNotFoundException($"Schema file not found: {schemaFilePath}");
            }

            var jsonContent = File.ReadAllText(schemaFilePath);
            var schemaDoc = System.Text.Json.JsonDocument.Parse(jsonContent);
            var root = schemaDoc.RootElement;

            var result = new ImportedSchemaInfo();

            // Helper method to process an object
            ImportedObjectInfo ProcessObject(JsonElement obj, string objectType)
            {
                var objInfo = new ImportedObjectInfo();
                
                if (obj.TryGetProperty("name", out var nameElement))
                    objInfo.Name = nameElement.GetString() ?? "";
                
                if (obj.TryGetProperty("schema", out var schemaElement))
                    objInfo.Schema = schemaElement.GetString() ?? "";
                
                objInfo.FullName = string.IsNullOrEmpty(objInfo.Schema) ? objInfo.Name : $"{objInfo.Schema}.{objInfo.Name}";

                // Parse columns
                if (obj.TryGetProperty("columns", out var columnsArray))
                {
                    foreach (var col in columnsArray.EnumerateArray())
                    {
                        var colInfo = new ImportedColumnInfo();
                        if (col.TryGetProperty("name", out var colNameElement))
                            colInfo.Name = colNameElement.GetString() ?? "";
                        if (col.TryGetProperty("dataType", out var dataTypeElement))
                            colInfo.DataType = dataTypeElement.GetString() ?? "";
                        if (col.TryGetProperty("description", out var descElement))
                            colInfo.Description = descElement.GetString() ?? "";
                        
                        objInfo.Columns.Add(colInfo);
                    }
                }

                // Parse dependencies
                if (obj.TryGetProperty("dependencies", out var dependenciesArray))
                {
                    foreach (var dep in dependenciesArray.EnumerateArray())
                    {
                        if (dep.ValueKind == System.Text.Json.JsonValueKind.String)
                        {
                            objInfo.Dependencies.Add(dep.GetString() ?? "");
                        }
                    }
                }

                // Parse metadata
                if (obj.TryGetProperty("metadata", out var metadataObj))
                {
                    foreach (var prop in metadataObj.EnumerateObject())
                    {
                        objInfo.Metadata[prop.Name] = prop.Value.ToString();
                    }
                }

                return objInfo;
            }

            // Check for new format with separate arrays
            if (root.TryGetProperty("tables", out var tablesArray))
            {
                foreach (var obj in tablesArray.EnumerateArray())
                {
                    var objInfo = ProcessObject(obj, "table");
                    result.Tables[objInfo.FullName] = objInfo;
                }
            }

            if (root.TryGetProperty("views", out var viewsArray))
            {
                foreach (var obj in viewsArray.EnumerateArray())
                {
                    var objInfo = ProcessObject(obj, "view");
                    result.Views[objInfo.FullName] = objInfo;
                }
            }

            if (root.TryGetProperty("functions", out var functionsArray))
            {
                foreach (var obj in functionsArray.EnumerateArray())
                {
                    var objInfo = ProcessObject(obj, "function");
                    result.Functions[objInfo.FullName] = objInfo;
                }
            }

            if (root.TryGetProperty("storedProcedures", out var storedProceduresArray))
            {
                foreach (var obj in storedProceduresArray.EnumerateArray())
                {
                    var objInfo = ProcessObject(obj, "storedprocedure");
                    result.StoredProcedures[objInfo.FullName] = objInfo;
                }
            }

            // Also check for legacy format with "objects" array
            if (root.TryGetProperty("objects", out var objectsArray))
            {
                foreach (var obj in objectsArray.EnumerateArray())
                {
                    var objInfo = ProcessObject(obj, "");
                    
                    // Add to appropriate collection based on type
                    if (obj.TryGetProperty("type", out var typeElement))
                    {
                        var objectType = typeElement.GetString()?.ToLowerInvariant();
                        switch (objectType)
                        {
                            case "table":
                                result.Tables[objInfo.FullName] = objInfo;
                                break;
                            case "view":
                                result.Views[objInfo.FullName] = objInfo;
                                break;
                            case "function":
                                result.Functions[objInfo.FullName] = objInfo;
                                break;
                            case "storedprocedure":
                                result.StoredProcedures[objInfo.FullName] = objInfo;
                                break;
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Enhances discovered SQL objects with information from imported schema.
        /// </summary>
        static List<SqlObjectDefinition> EnhanceObjectsWithImportedSchema(
            List<SqlObjectDefinition> discoveredObjects, 
            ImportedSchemaInfo importedSchema)
        {
            var enhancedObjects = new List<SqlObjectDefinition>();

            foreach (var obj in discoveredObjects)
            {
                // Create a copy of the object
                var enhanced = new SqlObjectDefinition
                {
                    Name = obj.Name,
                    Schema = obj.Schema,
                    Type = obj.Type,
                    FilePath = obj.FilePath,
                    SqlContent = obj.SqlContent,
                    Dependencies = new List<string>(obj.Dependencies),
                    Parameters = new List<string>(obj.Parameters),
                    Columns = new List<string>(obj.Columns),
                    Metadata = new Dictionary<string, object>(obj.Metadata),
                    DatasetNamespace = obj.DatasetNamespace,
                    ColumnDefinitions = new List<SqlColumnDefinition>(obj.ColumnDefinitions)
                };

                // Find matching imported object
                ImportedObjectInfo? importedInfo = null;
                switch (obj.Type)
                {
                    case SqlObjectType.Table:
                        importedSchema.Tables.TryGetValue(obj.FullName, out importedInfo);
                        break;
                    case SqlObjectType.View:
                        importedSchema.Views.TryGetValue(obj.FullName, out importedInfo);
                        break;
                    case SqlObjectType.Function:
                        importedSchema.Functions.TryGetValue(obj.FullName, out importedInfo);
                        break;
                    case SqlObjectType.StoredProcedure:
                        importedSchema.StoredProcedures.TryGetValue(obj.FullName, out importedInfo);
                        break;
                }

                // Enhance with imported information if available
                if (importedInfo != null)
                {
                    // Enhance column definitions
                    enhanced.ColumnDefinitions.Clear();
                    foreach (var importedCol in importedInfo.Columns)
                    {
                        enhanced.ColumnDefinitions.Add(new SqlColumnDefinition
                        {
                            Name = importedCol.Name,
                            DataType = importedCol.DataType,
                            Description = importedCol.Description,
                            Tags = importedCol.Tags ?? new Dictionary<string, object>()
                        });
                    }

                    // Update column names list for compatibility
                    enhanced.Columns = importedInfo.Columns.Select(c => c.Name).ToList();

                    // Merge dependencies (avoid duplicates)
                    var allDeps = enhanced.Dependencies.Concat(importedInfo.Dependencies).Distinct().ToList();
                    enhanced.Dependencies = allDeps;

                    // Merge metadata
                    foreach (var kvp in importedInfo.Metadata)
                    {
                        enhanced.Metadata[$"imported_{kvp.Key}"] = kvp.Value;
                    }

                    enhanced.Metadata["enhanced_with_imported_schema"] = true;
                }

                enhancedObjects.Add(enhanced);
            }

            return enhancedObjects;
        }

        /// <summary>
        /// Enhanced project provider that wraps an existing provider and adds imported schema information.
        /// </summary>
        public class EnhancedSsdtProjectProvider : IProjectProvider
        {
            private readonly IProjectProvider _baseProvider;
            private readonly ImportedSchemaInfo _importedSchema;

            public EnhancedSsdtProjectProvider(IProjectProvider baseProvider, ImportedSchemaInfo importedSchema)
            {
                _baseProvider = baseProvider ?? throw new ArgumentNullException(nameof(baseProvider));
                _importedSchema = importedSchema ?? throw new ArgumentNullException(nameof(importedSchema));
            }

            public IEnumerable<SqlObjectDefinition> DiscoverSqlObjects(string projectPath)
            {
                var baseObjects = _baseProvider.DiscoverSqlObjects(projectPath).ToList();
                return EnhanceObjectsWithImportedSchema(baseObjects, _importedSchema);
            }
        }
    }

    /// <summary>
    /// SQL DOM visitor that extracts analyzable SQL statements from CREATE statements in single files.
    /// </summary>
    internal class SingleFileAnalyzableSqlExtractor : TSqlFragmentVisitor
    {
        private readonly StringBuilder _extractedSql = new StringBuilder();
        private bool _insideFunction = false;
        
        public string ExtractedSql => _extractedSql.ToString();

        public override void Visit(CreateViewStatement node)
        {
            if (node.SelectStatement != null)
            {
                // Extract the SELECT statement from the CREATE VIEW
                ExtractSelectStatement(node.SelectStatement);
            }
        }

        public override void Visit(CreateProcedureStatement node)
        {
            if (node.StatementList?.Statements != null)
            {
                // Extract all data modification statements from the procedure body
                foreach (var statement in node.StatementList.Statements)
                {
                    ProcessIndividualStatement(statement);
                }
            }
        }

        public override void Visit(CreateFunctionStatement node)
        {
            // For table-valued functions, set flag and let visitor pattern handle SelectStatement
            if (node.ReturnType is SelectFunctionReturnType)
            {
                _insideFunction = true;
                base.Visit(node);  // This will visit children including SelectStatement
                _insideFunction = false;
            }
        }

        public override void Visit(SelectStatement node)
        {
            if (_insideFunction)
            {
                // This is a SELECT statement inside a table-valued function
                ExtractSelectStatement(node);
            }
            else
            {
                // Handle standalone SELECT statements normally
                ExtractSelectStatement(node);
            }
        }

        public override void Visit(ReturnStatement node)
        {
            if (node.Expression is ScalarSubquery subquery)
            {
                ExtractSelectStatement(subquery.QueryExpression);
            }
            // Don't call base.Visit to avoid duplicate processing
        }

        private void ExtractSelectStatement(TSqlFragment selectFragment)
        {
            if (selectFragment != null)
            {
                // Convert the SQL fragment back to text
                var generator = new Sql160ScriptGenerator();
                string sql;
                generator.GenerateScript(selectFragment, out sql);
                
                if (!string.IsNullOrEmpty(sql))
                {
                    if (_extractedSql.Length > 0)
                    {
                        _extractedSql.AppendLine();
                        _extractedSql.AppendLine("-- Next SELECT statement:");
                    }
                    _extractedSql.AppendLine(sql);
                }
            }
        }

        private void ExtractDataModificationStatement(TSqlStatement statement)
        {
            if (statement != null)
            {
                // Convert the data modification statement (INSERT/UPDATE/MERGE) to text
                var generator = new Sql160ScriptGenerator();
                string sql;
                generator.GenerateScript(statement, out sql);
                
                if (!string.IsNullOrEmpty(sql))
                {
                    if (_extractedSql.Length > 0)
                    {
                        _extractedSql.AppendLine();
                        _extractedSql.AppendLine($"-- Next {statement.GetType().Name.Replace("Statement", "").ToUpper()} statement:");
                    }
                    _extractedSql.AppendLine(sql);
                }
            }
        }

        private void ExtractFromIfStatement(IfStatement ifStmt)
        {
            if (ifStmt.ThenStatement != null)
            {
                ProcessIndividualStatement(ifStmt.ThenStatement);
            }
            if (ifStmt.ElseStatement != null)
            {
                ProcessIndividualStatement(ifStmt.ElseStatement);
            }
        }

        private void ExtractFromWhileStatement(WhileStatement whileStmt)
        {
            if (whileStmt.Statement != null)
            {
                ProcessIndividualStatement(whileStmt.Statement);
            }
        }

        private void ExtractFromTryCatchStatement(TryCatchStatement tryCatchStmt)
        {
            if (tryCatchStmt.TryStatements?.Statements != null)
            {
                foreach (var stmt in tryCatchStmt.TryStatements.Statements)
                {
                    ProcessIndividualStatement(stmt);
                }
            }
            
            if (tryCatchStmt.CatchStatements?.Statements != null)
            {
                foreach (var stmt in tryCatchStmt.CatchStatements.Statements)
                {
                    ProcessIndividualStatement(stmt);
                }
            }
        }

        private void ExtractFromStatementList(StatementList statementList)
        {
            if (statementList?.Statements != null)
            {
                foreach (var stmt in statementList.Statements)
                {
                    ProcessIndividualStatement(stmt);
                }
            }
        }

        private void ProcessIndividualStatement(TSqlStatement statement)
        {
            if (statement is SelectStatement selectStmt)
            {
                ExtractSelectStatement(selectStmt);
            }
            else if (statement is InsertStatement insertStmt)
            {
                ExtractDataModificationStatement(insertStmt);
            }
            else if (statement is UpdateStatement updateStmt)
            {
                ExtractDataModificationStatement(updateStmt);
            }
            else if (statement is MergeStatement mergeStmt)
            {
                ExtractDataModificationStatement(mergeStmt);
            }
            else if (statement is IfStatement ifStmt)
            {
                ExtractFromIfStatement(ifStmt);
            }
            else if (statement is WhileStatement whileStmt)
            {
                ExtractFromWhileStatement(whileStmt);
            }
            else if (statement is TryCatchStatement tryCatchStmt)
            {
                ExtractFromTryCatchStatement(tryCatchStmt);
            }
            else if (statement is BeginEndBlockStatement blockStmt)
            {
                ExtractFromStatementList(blockStmt.StatementList);
            }
        }
    }
}
