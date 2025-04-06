﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿﻿using System;
using System.IO;
using System.Linq; // Added for Any()
using ColumnLineageCore;
using ColumnLineageCore.Interfaces; // For IAstProvider, IProcessorFactory
using ColumnLineageCore.Interfaces.Processors; // For ICteScopeProcessor
using ColumnLineageCore.Processors; // For concrete processors
using ColumnLineageCore.Helpers; // Added for FileSystemViewDefinitionProvider
using Microsoft.SqlServer.TransactSql.ScriptDom; // For TSqlFragment types

namespace ColumnLineageCli
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Usage: ColumnLineageCli <path_to_sql_file>");
                return;
            }

            string filePath = args[0];
            if (!File.Exists(filePath))
            {
                Console.WriteLine($"Error: File not found at '{filePath}'");
                return;
            }

            try
            {
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

                // --- Perform Analysis ---
                Console.WriteLine($"Analyzing script: {filePath}...");
                var lineageResult = analyzer.Analyze(sqlScript); // Use the new analyzer

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
            // Register ExecuteStatementProcessor (requires IAstProvider)
            factory.RegisterProcessorInstance<ExecuteStatement>(new ExecuteStatementProcessor(astProvider));
            // Add DELETE, MERGE etc. here when implemented

            Console.WriteLine("Processor registration complete.");
            return factory;
        }
    }
}
