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
                string? targetTableName = null;
                
                // Handle different types of table references for INSERT target
                switch (statement.InsertSpecification.Target)
                {
                    case NamedTableReference namedTable:
                        targetTableName = namedTable.SchemaObject?.BaseIdentifier?.Value;
                        break;
                    case VariableTableReference variableTable:
                        targetTableName = variableTable.Variable?.Name;
                        break;
                }
                
                if (string.IsNullOrEmpty(targetTableName))
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Warning: Could not determine target table name for INSERT statement.");
                    return;
                }

                System.Diagnostics.Debug.WriteLine($"[Processor] Processing INSERT INTO {targetTableName} SELECT...");

                // Get the target columns specified in the INSERT statement
                var targetColumns = statement.InsertSpecification.Columns?.Select(c => c.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value).Where(name => name != null).ToList();

                // Process the source SELECT query to get its output structure and lineage
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

                        // Create the target node for the table variable/table
                        var targetNode = new ColumnNode(targetColName, targetTableName);
                        context.Graph.AddNode(targetNode);

                        if (sourceCol.SourceNode != null) 
                        {
                            // Direct source node available - create direct edge
                            context.Graph.AddNode(sourceCol.SourceNode); // Ensure source node exists
                            context.Graph.AddEdge(sourceCol.SourceNode, targetNode);
                            System.Diagnostics.Debug.WriteLine($"[Processor] Added INSERT lineage edge: {sourceCol.SourceNode.Id} -> {targetNode.Id}");
                        }
                        else
                        {
                            // No direct source node (complex expression like CASE) - find the output node that was created by SELECT processing
                            var outputNode = context.Graph.Nodes
                                .FirstOrDefault(n => n.SourceName == null && n.Name == sourceCol.OutputName);
                            
                            if (outputNode != null)
                            {
                                // Found the output node from SELECT processing - connect it to the INSERT target
                                context.Graph.AddEdge(outputNode, targetNode);
                                System.Diagnostics.Debug.WriteLine($"[Processor] Added INSERT lineage edge via output node: {outputNode.Id} -> {targetNode.Id}");
                            }
                            else
                            {
                                System.Diagnostics.Debug.WriteLine($"[Processor] Skipping INSERT lineage edge for target '{targetTableName}.{targetColName}' because output node '{sourceCol.OutputName}' not found.");
                            }
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
                          // Process the SELECT query normally (not as subquery) to get full lineage
                          var result = queryProcessor.ProcessQuery(querySpec, context);
                          return result;
                      }
                  }
                  else if (queryExpression is BinaryQueryExpression binaryQuery)
                  {
                      var processor = context.ProcessorFactory.GetProcessor(binaryQuery);
                      if (processor is IQueryExpressionProcessor<BinaryQueryExpression> queryProcessor)
                      {
                          // Process the binary query normally (not as subquery) to get full lineage
                          var result = queryProcessor.ProcessQuery(binaryQuery, context);
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
