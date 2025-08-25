using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Helpers;
using ColumnLineageCore.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ColumnLineageCore
{
    /// <summary>
    /// Orchestrates column lineage analysis for entire SSDT projects.
    /// </summary>
    public class ProjectLineageAnalyzer
    {
        private readonly IAstProvider _astProvider;
        private readonly IProcessorFactory _processorFactory;
        private readonly ICteScopeProcessor _cteScopeProcessor;
        private readonly IProjectProvider _projectProvider;

        public ProjectLineageAnalyzer(
            IAstProvider astProvider,
            IProcessorFactory processorFactory,
            ICteScopeProcessor cteScopeProcessor,
            IProjectProvider projectProvider)
        {
            _astProvider = astProvider ?? throw new ArgumentNullException(nameof(astProvider));
            _processorFactory = processorFactory ?? throw new ArgumentNullException(nameof(processorFactory));
            _cteScopeProcessor = cteScopeProcessor ?? throw new ArgumentNullException(nameof(cteScopeProcessor));
            _projectProvider = projectProvider ?? throw new ArgumentNullException(nameof(projectProvider));
        }

        /// <summary>
        /// Analyzes column lineage for an entire SSDT project.
        /// </summary>
        /// <param name="projectPath">Path to the SSDT project file (.sqlproj) or project directory.</param>
        /// <returns>A comprehensive lineage result covering the entire project.</returns>
        public ProjectLineageResult AnalyzeProject(string projectPath)
        {
            var result = new ProjectLineageResult();
            
            try
            {
                // Step 1: Discover all SQL objects in the project
                var sqlObjects = _projectProvider.DiscoverSqlObjects(projectPath).ToList();
                result.DiscoveredObjects = sqlObjects;

                Console.WriteLine($"Discovered {sqlObjects.Count} SQL objects in project:");
                foreach (var group in sqlObjects.GroupBy(o => o.Type))
                {
                    Console.WriteLine($"  {group.Key}: {group.Count()} objects");
                }

                // Step 2: Create SSDT-aware view definition provider
                var ssdtViewProvider = new SsdtViewDefinitionProvider(sqlObjects);
                
                // Step 3: Create a new analyzer with the SSDT view provider
                var projectAnalyzer = new LineageAnalyzer(
                    _astProvider,
                    _processorFactory,
                    _cteScopeProcessor,
                    ssdtViewProvider);

                // Step 4: Process objects in dependency order
                var processedObjects = ProcessObjectsInOrder(sqlObjects, projectAnalyzer);
                result.ProcessedObjects = processedObjects;

                // Step 5: Combine all lineage results
                result.CombinedLineage = CombineLineageResults(processedObjects.Select(p => p.LineageResult));

                Console.WriteLine($"\nProject Analysis Summary:");
                Console.WriteLine($"  Total Nodes: {result.CombinedLineage.Nodes.Count}");
                Console.WriteLine($"  Total Edges: {result.CombinedLineage.Edges.Count}");
                Console.WriteLine($"  Objects with Errors: {processedObjects.Count(p => p.LineageResult.Errors.Any())}");

            }
            catch (Exception ex)
            {
                result.ProjectErrors.Add($"Project analysis failed: {ex.Message}");
                Console.WriteLine($"Error analyzing project: {ex.Message}");
            }

            return result;
        }

        private List<ProcessedObject> ProcessObjectsInOrder(
            List<SqlObjectDefinition> sqlObjects, 
            LineageAnalyzer analyzer)
        {
            var processedObjects = new List<ProcessedObject>();
            var processed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // Process schemas first
            foreach (var schema in sqlObjects.Where(o => o.Type == SqlObjectType.Schema))
            {
                ProcessSqlObject(schema, analyzer, processedObjects, processed);
            }

            // Process tables next (they typically have no dependencies)
            foreach (var table in sqlObjects.Where(o => o.Type == SqlObjectType.Table))
            {
                ProcessSqlObject(table, analyzer, processedObjects, processed);
            }

            // Process views (may depend on tables and other views)
            // Sort by dependency depth to process simpler views first
            var views = sqlObjects.Where(o => o.Type == SqlObjectType.View)
                .OrderBy(v => v.Dependencies.Count)
                .ToList();
            
            foreach (var view in views)
            {
                ProcessSqlObject(view, analyzer, processedObjects, processed);
            }

            // Process stored procedures and functions
            foreach (var proc in sqlObjects.Where(o => o.Type == SqlObjectType.StoredProcedure))
            {
                ProcessSqlObject(proc, analyzer, processedObjects, processed);
            }

            foreach (var func in sqlObjects.Where(o => o.Type == SqlObjectType.Function))
            {
                ProcessSqlObject(func, analyzer, processedObjects, processed);
            }

            return processedObjects;
        }

        private void ProcessSqlObject(
            SqlObjectDefinition sqlObject,
            LineageAnalyzer analyzer,
            List<ProcessedObject> processedObjects,
            HashSet<string> processed)
        {
            if (processed.Contains(sqlObject.FullName))
                return;

            try
            {
                Console.WriteLine($"Processing {sqlObject.Type}: {sqlObject.FullName}");
                
                LineageResult lineageResult;
                
                // Extract analyzable SQL based on object type
                var analyzableSql = ExtractAnalyzableSql(sqlObject);
                
                if (!string.IsNullOrEmpty(analyzableSql))
                {
                    lineageResult = analyzer.Analyze(analyzableSql);
                }
                else
                {
                    // For objects without analyzable SQL (like tables), create empty result
                    lineageResult = new LineageResult();
                }
                
                processedObjects.Add(new ProcessedObject
                {
                    SqlObject = sqlObject,
                    LineageResult = lineageResult
                });

                processed.Add(sqlObject.FullName);

                if (lineageResult.Errors.Any())
                {
                    Console.WriteLine($"  Warnings: {lineageResult.Errors.Count} parsing errors");
                }
                
                Console.WriteLine($"  Lineage: {lineageResult.Nodes.Count} nodes, {lineageResult.Edges.Count} edges");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"  Error processing {sqlObject.FullName}: {ex.Message}");
                
                processedObjects.Add(new ProcessedObject
                {
                    SqlObject = sqlObject,
                    LineageResult = new LineageResult 
                    { 
                        Errors = new List<ParseError> 
                        { 
                            new ParseError(0, 0, 0, 0, $"Processing error: {ex.Message}") 
                        } 
                    }
                });
            }
        }

        /// <summary>
        /// Extracts SQL statements that can be analyzed for lineage from different object types.
        /// </summary>
        private string ExtractAnalyzableSql(SqlObjectDefinition sqlObject)
        {
            try
            {
                var parser = new TSql160Parser(true);
                IList<ParseError> errors;
                var fragment = parser.Parse(new StringReader(sqlObject.SqlContent), out errors);
                
                if (fragment == null || errors.Count > 0)
                {
                    // If parsing fails, return null - can't extract analyzable SQL
                    return null;
                }

                var extractor = new AnalyzableSqlExtractor();
                fragment.Accept(extractor);
                
                return extractor.ExtractedSql;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"[ProjectLineageAnalyzer] Error extracting SQL from {sqlObject.FullName}: {ex.Message}");
                return null;
            }
        }

        private LineageResult CombineLineageResults(IEnumerable<LineageResult> results)
        {
            var combinedNodes = new Dictionary<string, ColumnNode>();
            var combinedEdges = new List<LineageEdge>();
            var combinedErrors = new List<ParseError>();

            foreach (var result in results)
            {
                // Merge nodes (avoid duplicates)
                foreach (var node in result.Nodes)
                {
                    if (!combinedNodes.ContainsKey(node.Id))
                    {
                        combinedNodes[node.Id] = node;
                    }
                }

                // Add all edges
                combinedEdges.AddRange(result.Edges);

                // Add all errors
                combinedErrors.AddRange(result.Errors);
            }

            return new LineageResult(
                combinedNodes.Values.ToList(), 
                combinedEdges, 
                combinedErrors
            );
        }
    }

    /// <summary>
    /// Result of analyzing an entire SSDT project.
    /// </summary>
    public class ProjectLineageResult
    {
        public List<SqlObjectDefinition> DiscoveredObjects { get; set; } = new();
        public List<ProcessedObject> ProcessedObjects { get; set; } = new();
        public LineageResult CombinedLineage { get; set; } = new();
        public List<string> ProjectErrors { get; set; } = new();
    }

    /// <summary>
    /// Represents a processed SQL object and its lineage analysis result.
    /// </summary>
    public class ProcessedObject
    {
        public SqlObjectDefinition SqlObject { get; set; } = null!;
        public LineageResult LineageResult { get; set; } = null!;
    }

    /// <summary>
    /// SQL DOM visitor that extracts analyzable SQL statements from CREATE VIEW and CREATE PROCEDURE statements.
    /// </summary>
    internal class AnalyzableSqlExtractor : TSqlFragmentVisitor
    {
        private readonly StringBuilder _extractedSql = new StringBuilder();
        
        public string ExtractedSql => _extractedSql.ToString();

        public override void Visit(CreateViewStatement node)
        {
            if (node.SelectStatement != null)
            {
                // Extract the SELECT statement from the CREATE VIEW
                ExtractSelectStatement(node.SelectStatement);
            }
            // Don't visit children to avoid duplicate processing
        }

        public override void Visit(CreateProcedureStatement node)
        {
            if (node.StatementList?.Statements != null)
            {
                // Extract all data modification statements from the procedure body
                foreach (var statement in node.StatementList.Statements)
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
                        // Handle all statements inside IF blocks
                        ExtractFromIfStatement(ifStmt);
                    }
                    else if (statement is WhileStatement whileStmt)
                    {
                        // Handle all statements inside WHILE blocks
                        ExtractFromWhileStatement(whileStmt);
                    }
                    else if (statement is TryCatchStatement tryCatchStmt)
                    {
                        // Handle all statements inside TRY/CATCH blocks
                        ExtractFromTryCatchStatement(tryCatchStmt);
                    }
                }
            }
            // Don't visit children to avoid duplicate processing
        }

        public override void Visit(CreateFunctionStatement node)
        {
            // For table-valued functions, extract the SELECT statement
            if (node.ReturnType is SelectFunctionReturnType && node.StatementList?.Statements != null)
            {
                foreach (var statement in node.StatementList.Statements)
                {
                    if (statement is ReturnStatement returnStmt && 
                        returnStmt.Expression is ScalarSubquery subquery)
                    {
                        ExtractSelectStatement(subquery.QueryExpression);
                    }
                    else if (statement is SelectStatement selectStmt)
                    {
                        ExtractSelectStatement(selectStmt);
                    }
                }
            }
            // Don't visit children to avoid duplicate processing
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
            ExtractFromStatementList(ifStmt.ThenStatement);
            ExtractFromStatementList(ifStmt.ElseStatement);
        }

        private void ExtractFromWhileStatement(WhileStatement whileStmt)
        {
            ExtractFromStatementList(whileStmt.Statement);
        }

        private void ExtractFromTryCatchStatement(TryCatchStatement tryCatchStmt)
        {
            if (tryCatchStmt.TryStatements?.Statements != null)
            {
                foreach (var stmt in tryCatchStmt.TryStatements.Statements)
                {
                    ExtractStatement(stmt);
                }
            }
            
            if (tryCatchStmt.CatchStatements?.Statements != null)
            {
                foreach (var stmt in tryCatchStmt.CatchStatements.Statements)
                {
                    ExtractStatement(stmt);
                }
            }
        }

        private void ExtractFromStatementList(TSqlStatement statement)
        {
            if (statement == null) return;

            if (statement is BeginEndBlockStatement blockStmt && blockStmt.StatementList?.Statements != null)
            {
                foreach (var stmt in blockStmt.StatementList.Statements)
                {
                    ExtractStatement(stmt);
                }
            }
            else
            {
                ExtractStatement(statement);
            }
        }

        private void ExtractStatement(TSqlStatement statement)
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
        }
    }
}
