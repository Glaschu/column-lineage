using ColumnLineageCore.Interfaces; // Ensure this is present
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Helpers;
using ColumnLineageCore.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ColumnLineageCore
{
    /// <summary>
    /// Orchestrates the column lineage analysis process using dedicated processors.
    /// Replaces the monolithic SqlParser.
    /// </summary>
    public class LineageAnalyzer
    {
        private readonly IAstProvider _astProvider;
        private readonly IProcessorFactory _processorFactory;
        private readonly ICteScopeProcessor _cteScopeProcessor;
        private readonly IViewDefinitionProvider _viewDefinitionProvider;
        private readonly Dictionary<string, LineageResult> _viewAnalysisCache;

        // Keep provider and cache references for now, needed by processors until context refactor is complete
        // TODO: Remove these fields once processors access them via context

        public LineageAnalyzer(
            IAstProvider astProvider,
            IProcessorFactory processorFactory,
            ICteScopeProcessor cteScopeProcessor,
            IViewDefinitionProvider viewDefinitionProvider) // Added provider dependency
        {
            _astProvider = astProvider ?? throw new ArgumentNullException(nameof(astProvider));
            _processorFactory = processorFactory ?? throw new ArgumentNullException(nameof(processorFactory));
            _cteScopeProcessor = cteScopeProcessor ?? throw new ArgumentNullException(nameof(cteScopeProcessor));
            _viewDefinitionProvider = viewDefinitionProvider ?? throw new ArgumentNullException(nameof(viewDefinitionProvider)); // Added
            _viewAnalysisCache = new Dictionary<string, LineageResult>(StringComparer.OrdinalIgnoreCase);

            // TODO: Register all the implemented processors with the factory.
            // This should ideally happen during application startup / dependency injection setup.
            // Example (needs concrete factory instance):
            // if (_processorFactory is ProcessorFactory concreteFactory) // Assuming concrete type for registration example
            // {
            //     concreteFactory.RegisterProcessor<NamedTableReference, Processors.NamedTableReferenceProcessor>();
            //     concreteFactory.RegisterProcessor<JoinTableReference, Processors.JoinTableReferenceProcessor>();
            //     concreteFactory.RegisterProcessor<QueryDerivedTable, Processors.QueryDerivedTableProcessor>();
            //     concreteFactory.RegisterProcessor<SelectScalarExpression, Processors.SelectScalarExpressionProcessor>();
            //     concreteFactory.RegisterProcessor<SelectStarExpression, Processors.SelectStarExpressionProcessor>();
            //     concreteFactory.RegisterProcessor<QuerySpecification, Processors.QuerySpecificationProcessor>();
            //     concreteFactory.RegisterProcessor<BinaryQueryExpression, Processors.BinaryQueryExpressionProcessor>();
            //     // Note: CteScopeProcessor is injected directly, not typically retrieved via factory for fragments.
            //     // Register other processors (Statement processors etc.) as they are implemented.
            // }
        }

        /// <summary>
        /// Parses the SQL script and analyzes column lineage.
        /// </summary>
        /// <param name="sqlScript">The SQL script text.</param>
        /// <returns>A LineageResult containing the analyzed graph.</returns>
        public LineageResult Analyze(string sqlScript)
        {
            // Clear cache for each top-level analysis run
            _viewAnalysisCache.Clear();

            var lineageGraph = new LineageGraph();
            // Pass provider and cache to the context
            var processingContext = new ProcessingContext(lineageGraph, _processorFactory, _viewDefinitionProvider, _viewAnalysisCache);


            // 1. Parse SQL to AST
            TSqlFragment? fragment = _astProvider.Parse(sqlScript, out IList<ParseError> errors);

            if (errors.Any())
            {
                // Handle parsing errors (e.g., log, throw, return partial result)
                System.Diagnostics.Debug.WriteLine($"[Analyzer] Parse errors encountered: {errors.Count}. Analysis may be incomplete.");
                // Optionally, return early or throw:
                // return new LineageResult { Errors = errors };
            }

            if (fragment == null)
            {
                 System.Diagnostics.Debug.WriteLine("[Analyzer] Parsing returned null fragment.");
                 return new LineageResult { Errors = errors }; // Return empty result with errors
            }

            // 2. Process the AST
            if (fragment is TSqlScript script)
            {
                // Script-level CTEs are handled when processing the first statement that contains them.
                // Remove direct check on script.WithCtesAndXmlNamespaces.

                // Process statements in batches
                foreach (var batch in script.Batches)
                {
                    foreach (var statement in batch.Statements)
                    {
                        ProcessStatement(statement, processingContext);
                    }
                }

                 // No need to explicitly pop script-level scope here,
                 // as statement-level scopes are handled by ProcessStatement/CteScopePopper.
                 // The base context scope remains until analysis is complete.
            }
            else
            {
                 // Handle cases where the root fragment isn't a TSqlScript (e.g., single statement parse)
                 if (fragment is TSqlStatement statement)
                 {
                      ProcessStatement(statement, processingContext);
                 }
                 else
                 {
                      System.Diagnostics.Debug.WriteLine($"[Analyzer] Warning: Unsupported root fragment type: {fragment.GetType().Name}");
                 }
            }

            // 3. Return result
            // The lineageGraph instance within the context holds the final nodes and edges.
            return new LineageResult(lineageGraph.Nodes.ToList(), lineageGraph.Edges.ToList(), errors);
        }


        /// <summary>
        /// Processes a single TSqlStatement using the appropriate processor from the factory.
        /// </summary>
        private void ProcessStatement(TSqlStatement statement, IProcessingContext context)
        {
             System.Diagnostics.Debug.WriteLine($"[Analyzer] Processing statement type: {statement.GetType().Name}");
             IDisposable? cteScopeHandle = null;
             string? originalIntoTarget = null; // To restore context

             try
             {
                 // Handle statement-level WITH clauses before processing the main statement body
                 if (statement is SelectStatement selectStmt && selectStmt.WithCtesAndXmlNamespaces != null)
                 {
                      System.Diagnostics.Debug.WriteLine("[Analyzer] Processing statement-level WITH clause for SELECT...");
                      _cteScopeProcessor.ProcessCteScope(selectStmt.WithCtesAndXmlNamespaces.CommonTableExpressions, context);
                      cteScopeHandle = new CteScopePopper(context); // Ensure scope is popped
                 }
                 // TODO: Add similar checks for other statements that support WITH (UPDATE, INSERT, DELETE, MERGE)

                 // Check for SELECT INTO
                 if (statement is SelectStatement selectStatement && selectStatement.Into != null)
                 {
                      originalIntoTarget = context.IntoClauseTarget; // Save current value (should be null)
                      context.IntoClauseTarget = selectStatement.Into.BaseIdentifier?.Value; // Set target table name
                      System.Diagnostics.Debug.WriteLine($"[Analyzer] Detected SELECT INTO '{context.IntoClauseTarget}'.");
                 }

                 // Get and execute the processor for the specific statement type or its main component
                 if (statement is SelectStatement selectStatementForQuery) // Use different variable name
                 {
                      // Process the main query expression within the select statement
                      if (selectStatementForQuery.QueryExpression != null)
                      {
                           System.Diagnostics.Debug.WriteLine($"[Analyzer] Processing QueryExpression of type: {selectStatementForQuery.QueryExpression.GetType().Name}"); // Use selectStatementForQuery
                           // Use the helper method to process any QueryExpression type
                           ProcessAnyQueryExpression(selectStatementForQuery.QueryExpression, context); // Use selectStatementForQuery
                      }
                      else
                      {
                           System.Diagnostics.Debug.WriteLine($"[Analyzer] Warning: SelectStatement found without a QueryExpression.");
                      }
                 }
                 else if (statement is InsertStatement insertStatement)
                 {
                      _processorFactory.GetProcessor(insertStatement).Process(insertStatement, context);
                 }
                 else if (statement is UpdateStatement updateStatement)
                 {
                      _processorFactory.GetProcessor(updateStatement).Process(updateStatement, context);
                 }
                 else if (statement is MergeStatement mergeStatement)
                 {
                      _processorFactory.GetProcessor(mergeStatement).Process(mergeStatement, context);
                 }
                 else if (statement is ExecuteStatement executeStatement) // Added handling for EXECUTE
                 {
                      _processorFactory.GetProcessor(executeStatement).Process(executeStatement, context);
                 }
                 else if (statement is DeleteStatement deleteStatement) // Added handling for DELETE
                 {
                      _processorFactory.GetProcessor(deleteStatement).Process(deleteStatement, context);
                 }
                 // TODO: Add handling for other statement types as needed
                 // ... other statement types ...
                 else
                 {
                      System.Diagnostics.Debug.WriteLine($"[Analyzer] Warning: No specific processor registered or implemented for statement type: {statement.GetType().Name}. Skipping.");
                 }
             }
             catch (Exception ex)
             {
                  System.Diagnostics.Debug.WriteLine($"[Analyzer] Error processing statement: {ex.Message} - Statement Type: {statement.GetType().Name}");
                  // Decide on error handling (log, collect errors, re-throw?)
             }
             finally
             {
                  // Ensure statement-level CTE scope is popped if it was pushed
                  cteScopeHandle?.Dispose();
                  // Restore IntoClauseTarget if it was set
                  if (originalIntoTarget != context.IntoClauseTarget) // Check if we changed it
                  {
                       context.IntoClauseTarget = originalIntoTarget;
                       System.Diagnostics.Debug.WriteLine($"[Analyzer] Restored IntoClauseTarget.");
                  }
             }
        }

        // Helper method to process any QueryExpression type using the factory
        // This avoids duplicating the switch/if-else logic for QuerySpecification vs BinaryQueryExpression
        private void ProcessAnyQueryExpression(QueryExpression queryExpression, IProcessingContext context)
        {
             if (queryExpression is QuerySpecification querySpec)
             {
                  var processor = _processorFactory.GetProcessor(querySpec);
                  // Assuming IQueryExpressionProcessor defines ProcessQuery
                  if (processor is IQueryExpressionProcessor<QuerySpecification> queryProcessor)
                  {
                       queryProcessor.ProcessQuery(querySpec, context); // Call ProcessQuery
                  }
                  else { System.Diagnostics.Debug.WriteLine($"[Analyzer] Error: Processor for QuerySpecification does not implement IQueryExpressionProcessor<QuerySpecification>."); }
             }
             else if (queryExpression is BinaryQueryExpression binaryQuery)
             {
                  var processor = _processorFactory.GetProcessor(binaryQuery);
                  if (processor is IQueryExpressionProcessor<BinaryQueryExpression> queryProcessor)
                  {
                       queryProcessor.ProcessQuery(binaryQuery, context); // Call ProcessQuery
                  }
                  else { System.Diagnostics.Debug.WriteLine($"[Analyzer] Error: Processor for BinaryQueryExpression does not implement IQueryExpressionProcessor<BinaryQueryExpression>."); }
             }
             // Add other QueryExpression types (e.g., QueryParenthesisExpression) if needed
             else
             {
                  System.Diagnostics.Debug.WriteLine($"[Analyzer] Warning: Unsupported QueryExpression type encountered in ProcessAnyQueryExpression: {queryExpression.GetType().Name}");
             }
        }


        // Helper class for ensuring CTE scope is popped using 'using' statement
        private sealed class CteScopePopper : IDisposable
        {
            private readonly IProcessingContext _context;
            private bool _disposed = false;

            public CteScopePopper(IProcessingContext context)
            {
                _context = context;
            }

            public void Dispose()
            {
                if (!_disposed)
                {
                    _context.PopCteScope();
                    System.Diagnostics.Debug.WriteLine("[Analyzer] Popped statement-level CTE scope via Dispose.");
                    _disposed = true;
                }
            }
        }
    }
}
