using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Helpers;
using ColumnLineageCore.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Processes InsertStatement fragments, specifically focusing on INSERT INTO ... SELECT ...
    /// </summary>
    public class InsertStatementProcessor : IStatementProcessor<InsertStatement>
    {
        public void Process(InsertStatement statement, IProcessingContext context)
        {
            if (statement?.InsertSpecification?.Target == null) return; // Not a standard INSERT or target missing

            // Only handle INSERT ... SELECT ... for now
            if (statement.InsertSpecification.InsertSource is SelectInsertSource selectSource &&
                selectSource.Select != null)
            {
                string? targetTableName = (statement.InsertSpecification.Target as NamedTableReference)?.SchemaObject?.BaseIdentifier?.Value;
                if (string.IsNullOrEmpty(targetTableName))
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Warning: Could not determine target table name for INSERT statement.");
                    return;
                }

                System.Diagnostics.Debug.WriteLine($"[Processor] Processing INSERT INTO {targetTableName} SELECT...");

                // Get the target columns specified in the INSERT statement
                var targetColumns = statement.InsertSpecification.Columns?.Select(c => c.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value).Where(name => name != null).ToList();

                // Process the source SELECT query to get its output structure and lineage
                // We need a way to call the QueryExpression processing logic.
                // Let's assume LineageAnalyzer provides a helper or we replicate the logic.
                // For now, let's placeholder this call.
                List<OutputColumn> sourceOutputColumns = ProcessSourceQuery(selectSource.Select, context);

                if (sourceOutputColumns.Count == 0)
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Warning: Source SELECT for INSERT statement produced no output columns.");
                    return;
                }

                // Correlate source SELECT columns to target INSERT columns
                // If target columns are specified, match by order.
                // If target columns are NOT specified, assume SELECT order matches table definition order (requires schema info - skip for now).
                if (targetColumns != null)
                {
                    if (targetColumns.Count != sourceOutputColumns.Count)
                    {
                        System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Mismatch between target column count ({targetColumns.Count}) and source SELECT column count ({sourceOutputColumns.Count}) for INSERT INTO {targetTableName}.");
                        // Cannot reliably correlate - skip adding edges for this statement
                        return;
                    }

                    for (int i = 0; i < targetColumns.Count; i++)
                    {
                        string targetColName = targetColumns[i]!;
                        OutputColumn sourceCol = sourceOutputColumns[i];

                        if (sourceCol.SourceNode != null) // Only add edge if the source column has a traceable origin
                        {
                            var targetNode = new ColumnNode(targetColName, targetTableName);
                            context.Graph.AddNode(targetNode);
                            context.Graph.AddNode(sourceCol.SourceNode); // Ensure source node exists
                            context.Graph.AddEdge(sourceCol.SourceNode, targetNode);
                            System.Diagnostics.Debug.WriteLine($"[Processor] Added INSERT lineage edge: {sourceCol.SourceNode.Id} -> {targetNode.Id}");
                        }
                        else
                        {
                            System.Diagnostics.Debug.WriteLine($"[Processor] Skipping INSERT lineage edge for target '{targetTableName}.{targetColName}' because source '{sourceCol.OutputName}' has no traceable origin.");
                        }
                    }
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"[Processor] Warning: INSERT INTO {targetTableName} without explicit target columns is not fully supported without schema information. Lineage might be incomplete.");
                    // Basic attempt: Create target nodes but don't add edges without correlation
                    foreach(var sourceCol in sourceOutputColumns)
                    {
                         var targetNode = new ColumnNode(sourceCol.OutputName, targetTableName); // Guess target name = source name
                         context.Graph.AddNode(targetNode);
                    }
                }
            }
            else
            {
                System.Diagnostics.Debug.WriteLine("[Processor] Skipping INSERT statement (not INSERT...SELECT or source missing).");
            }
        }

        // Placeholder/Helper to process the source QueryExpression
        // In a real scenario, this might call back into LineageAnalyzer or use a shared helper
        private List<OutputColumn> ProcessSourceQuery(QueryExpression queryExpression, IProcessingContext context)
        {
             // This logic is similar to LineageAnalyzer.ProcessAnyQueryExpression
             // We need access to the processor factory from the context
             try
             {
                  if (queryExpression is QuerySpecification querySpec)
                  {
                      var processor = context.ProcessorFactory.GetProcessor(querySpec);
                      if (processor is IQueryExpressionProcessor<QuerySpecification> queryProcessor)
                      {
                          // IMPORTANT: Need to handle context flags (IsSubquery=true?) correctly if calling ProcessQuery directly
                          bool originalIsSubquery = context.IsSubquery;
                          context.IsSubquery = true; // Treat SELECT part of INSERT as a subquery context
                          var result = queryProcessor.ProcessQuery(querySpec, context);
                          context.IsSubquery = originalIsSubquery; // Restore flag
                          return result;
                      }
                  }
                  else if (queryExpression is BinaryQueryExpression binaryQuery)
                  {
                      var processor = context.ProcessorFactory.GetProcessor(binaryQuery);
                      if (processor is IQueryExpressionProcessor<BinaryQueryExpression> queryProcessor)
                      {
                          bool originalIsSubquery = context.IsSubquery;
                          context.IsSubquery = true;
                          var result = queryProcessor.ProcessQuery(binaryQuery, context);
                          context.IsSubquery = originalIsSubquery;
                          return result;
                      }
                  }
                   else if (queryExpression is QueryParenthesisExpression parenQuery && parenQuery.QueryExpression != null)
                   {
                        return ProcessSourceQuery(parenQuery.QueryExpression, context); // Recurse
                   }
             }
             catch (Exception ex)
             {
                  System.Diagnostics.Debug.WriteLine($"[Processor] Error processing source query for INSERT: {ex.Message}");
                  // Fall through to return empty list
             }

             System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported source query type for INSERT: {queryExpression.GetType().Name}");
             return new List<OutputColumn>();
        }
    }
}
