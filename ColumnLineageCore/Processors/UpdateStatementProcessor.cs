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
    /// Processes UpdateStatement fragments.
    /// Focuses on tracing lineage from source expressions/columns in the FROM/WHERE clauses
    /// to the target columns in the SET clause.
    /// </summary>
    public class UpdateStatementProcessor : IStatementProcessor<UpdateStatement>
    {
        public void Process(UpdateStatement statement, IProcessingContext context)
        {
            if (statement?.UpdateSpecification?.Target == null) return; // Invalid UPDATE statement

            string? targetTableName = (statement.UpdateSpecification.Target as NamedTableReference)?.SchemaObject?.BaseIdentifier?.Value;
            if (string.IsNullOrEmpty(targetTableName))
            {
                System.Diagnostics.Debug.WriteLine("[Processor] Warning: Could not determine target table name for UPDATE statement.");
                return;
            }

            System.Diagnostics.Debug.WriteLine($"[Processor] Processing UPDATE {targetTableName}...");

            // --- Manage Source Map Scope ---
            // Updates can have FROM clauses, so we need to manage the source map scope like in QuerySpecificationProcessor
            var originalSourceMap = context.CurrentSourceMap;
            var scopedSourceMap = new Dictionary<string, SourceInfo>(originalSourceMap, StringComparer.OrdinalIgnoreCase);
            context.CurrentSourceMap = scopedSourceMap;

            // Add the target table itself to the source map, often referenced by its own name
            // unless an alias is somehow applied (less common in UPDATE target).
            scopedSourceMap[targetTableName] = new SourceInfo(targetTableName, SourceType.Table);
            System.Diagnostics.Debug.WriteLine($"[Processor] Added target table '{targetTableName}' to local source map.");


            try
            {
                // --- Process FROM Clause (if exists) ---
                if (statement.UpdateSpecification.FromClause != null)
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Processing UPDATE FROM clause...");
                    foreach (var tableRef in statement.UpdateSpecification.FromClause.TableReferences)
                    {
                        ProcessTableReference(tableRef, context); // Use helper
                    }
                }

                // --- Process WHERE Clause (Optional - for finding sources used in conditions) ---
                if (statement.UpdateSpecification.WhereClause != null)
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] TODO: Process UPDATE WHERE clause...");
                    // VisitExpression(statement.UpdateSpecification.WhereClause.SearchCondition, context);
                }

                // --- Process SET Clauses ---
                if (statement.UpdateSpecification.SetClauses != null)
                {
                    System.Diagnostics.Debug.WriteLine("[Processor] Processing SET clauses...");
                    foreach (var setClause in statement.UpdateSpecification.SetClauses)
                    {
                        if (setClause is AssignmentSetClause assignment)
                        {
                            ProcessAssignmentSetClause(assignment, targetTableName, context);
                        }
                        // TODO: Handle other SetClause types if necessary (e.g., FunctionCallSetClause)
                    }
                }
            }
            finally
            {
                 // Restore original source map
                 context.CurrentSourceMap = originalSourceMap;
            }
        }

        /// <summary>
        /// Processes a single assignment clause (SET TargetCol = SourceExpr).
        /// </summary>
        private void ProcessAssignmentSetClause(AssignmentSetClause assignment, string targetTableName, IProcessingContext context)
        {
            if (assignment.Column?.MultiPartIdentifier?.Identifiers == null || !assignment.Column.MultiPartIdentifier.Identifiers.Any())
            {
                 System.Diagnostics.Debug.WriteLine("[Processor] Warning: Skipping SET clause with invalid target column.");
                 return;
            }

            // For now, assume target column identifier is simple (no schema/table prefix needed as it refers to the UPDATE target)
            string targetColumnName = assignment.Column.MultiPartIdentifier.Identifiers.Last().Value;
            var targetNode = new ColumnNode(targetColumnName, targetTableName);
            context.Graph.AddNode(targetNode); // Ensure target node exists

            System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing SET {targetNode.Id} = ...");

            if (assignment.NewValue == null) return; // No source expression

            // Find column references within the source expression
            var columnFinder = new ColumnReferenceFinder(); // Use the internal class directly
            assignment.NewValue.Accept(columnFinder);

            if (!columnFinder.ColumnReferences.Any())
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] No source column references found for SET {targetNode.Id}.");
                 return; // Literal or unhandled expression
            }

            // Resolve each source column reference and add edge
            foreach (var sourceColRef in columnFinder.ColumnReferences)
            {
                 // Use the ResolveColumnReferenceSource logic (maybe extract to a shared utility?)
                 // For now, replicate or call if accessible. Let's assume we have access or replicate.
                 ResolveSourceAndAddEdge(sourceColRef, targetNode, context);
            }
        }

        /// <summary>
        /// Helper to resolve a source ColumnReferenceExpression and add lineage edge to target.
        /// (This logic is similar to parts of SelectScalarExpressionProcessor.ResolveColumnReferenceSource and AddGraphElements)
        /// </summary>
        private void ResolveSourceAndAddEdge(ColumnReferenceExpression sourceColRef, ColumnNode targetNode, IProcessingContext context)
        {
             // Simplified resolution logic for UPDATE SET context
             // We primarily care about the *direct* source node in the current scope (FROM clause + target table)

             var parts = sourceColRef.MultiPartIdentifier?.Identifiers;
             if (parts == null || !parts.Any()) return;

             string sourceColumnName = parts.Last().Value;
             string? sourceIdentifier = parts.Count > 1 ? parts[parts.Count - 2].Value : null;

             SourceInfo? sourceInfo = null;
             string? resolvedSourceIdentifier = null;

             if (sourceIdentifier != null)
             {
                 // Qualified column
                 if (context.CurrentSourceMap.TryGetValue(sourceIdentifier, out sourceInfo))
                 {
                     resolvedSourceIdentifier = sourceIdentifier;
                 } else {
                      System.Diagnostics.Debug.WriteLine($"[Processor] UPDATE SET: Could not resolve source identifier '{sourceIdentifier}' for column '{sourceColumnName}'.");
                      return;
                 }
             }
             else if (context.CurrentSourceMap.Count == 1)
             {
                 // Unqualified, single source
                 var kvp = context.CurrentSourceMap.First();
                 sourceInfo = kvp.Value;
                 resolvedSourceIdentifier = kvp.Key;
             }
             else // Unqualified, multiple sources -> Need ambiguity check (Simplified for now)
             {
                  // Basic check: Does only one source *likely* provide this column?
                  // This is where BuildColumnAvailabilityMap would be useful if adapted for UPDATE context.
                  // For now, let's just try finding a non-target table source first.
                  var nonTargetSources = context.CurrentSourceMap
                        .Where(kvp => kvp.Value.Type != SourceType.Table || !kvp.Key.Equals(targetNode.SourceName, StringComparison.OrdinalIgnoreCase))
                        .ToList();

                  var potentialSources = context.CurrentSourceMap
                        .Where(kvp => SourceProvidesColumn(kvp.Value, sourceColumnName, context)) // Reuse obsolete helper for basic check
                        .ToList();

                  if (potentialSources.Count == 1) {
                       resolvedSourceIdentifier = potentialSources[0].Key;
                       sourceInfo = potentialSources[0].Value;
                       System.Diagnostics.Debug.WriteLine($"[Processor] UPDATE SET: Unqualified column '{sourceColumnName}' resolved to single source '{resolvedSourceIdentifier}'.");
                  } else {
                       System.Diagnostics.Debug.WriteLine($"[Processor] UPDATE SET: Ambiguous or unresolved unqualified column '{sourceColumnName}'. Potential Sources: {potentialSources.Count}.");
                       return; // Ambiguous or not found
                  }
             }

             // --- Determine Direct Source Node ---
             ColumnNode? directSourceNode = null;
             if (sourceInfo != null)
             {
                 switch (sourceInfo.Type)
                 {
                     case SourceType.Table:
                         directSourceNode = new ColumnNode(sourceColumnName, sourceInfo.Name);
                         break;
                     case SourceType.CTE:
                         directSourceNode = new ColumnNode(sourceColumnName, sourceInfo.Name); // Intermediate CTE node
                         break;
                     case SourceType.Subquery:
                         directSourceNode = new ColumnNode(sourceColumnName, sourceInfo.Name); // Intermediate Subquery node
                         break;
                 }
             }

             // --- Add Edge ---
             if (directSourceNode != null)
             {
                 context.Graph.AddNode(directSourceNode); // Ensure source node exists
                 context.Graph.AddEdge(directSourceNode, targetNode);
                 System.Diagnostics.Debug.WriteLine($"[Processor] Added UPDATE lineage edge: {directSourceNode.Id} -> {targetNode.Id}");
             }
        }


        // --- Helper Methods Copied/Adapted from QuerySpecificationProcessor ---

        /// <summary>
        /// Helper to process a TableReference using the factory.
        /// </summary>
        private void ProcessTableReference(TableReference tableReference, IProcessingContext context)
        {
            // (Same implementation as in QuerySpecificationProcessor)
            try
            {
                if (tableReference is NamedTableReference namedRef)
                {
                    context.ProcessorFactory.GetProcessor(namedRef).Process(namedRef, context);
                }
                else if (tableReference is QueryDerivedTable derivedRef)
                {
                     context.ProcessorFactory.GetProcessor(derivedRef).Process(derivedRef, context);
                }
                else if (tableReference is JoinTableReference joinRef)
                {
                     context.ProcessorFactory.GetProcessor(joinRef).Process(joinRef, context);
                }
                else
                {
                     System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported TableReference type in UPDATE FROM clause: {tableReference.GetType().Name}");
                }
            }
            catch (Exception ex)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Error processing TableReference in UPDATE: {ex.Message} - Type: {tableReference.GetType().Name}");
            }
        }

         // Copied from SelectScalarExpressionProcessor - needed for ambiguity check fallback
         private bool SourceProvidesColumn(SourceInfo sourceInfo, string columnName, IProcessingContext context)
        {
            switch (sourceInfo.Type)
            {
                case SourceType.Table:
                    return true; // Assume table provides any column for now
                case SourceType.CTE:
                    if (context.TryResolveCte(sourceInfo.Name, out var cteInfo) && cteInfo != null && cteInfo.IsProcessed)
                    {
                        return cteInfo.OutputColumnSources.ContainsKey(columnName);
                    }
                    return false;
                case SourceType.Subquery:
                    return sourceInfo.SubqueryOutputColumns?.Any(c => c.OutputName.Equals(columnName, StringComparison.OrdinalIgnoreCase)) ?? false;
                default:
                    return false;
            }
        }
    }
}
