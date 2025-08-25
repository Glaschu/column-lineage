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
    /// Processes QuerySpecification fragments (SELECT ... FROM ... WHERE ...).
    /// Orchestrates the processing of FROM, WHERE, SELECT clauses using other processors.
    /// </summary>
    public class QuerySpecificationProcessor : IQueryExpressionProcessor<QuerySpecification>
    {
        public List<OutputColumn> ProcessQuery(QuerySpecification querySpec, IProcessingContext context)
        {
            if (querySpec == null) throw new ArgumentNullException(nameof(querySpec));
            if (context == null) throw new ArgumentNullException(nameof(context));

            System.Diagnostics.Debug.WriteLine($"[Processor] Processing QuerySpecification...");

            // --- Manage Source Map Scope ---
            // --- Manage Source Map Scope ---
            // Save the current map, create a copy for this scope, and ensure restoration.
            var originalSourceMap = context.CurrentSourceMap;
            var scopedSourceMap = new Dictionary<string, SourceInfo>(originalSourceMap, StringComparer.OrdinalIgnoreCase); // Copy existing sources
            context.CurrentSourceMap = scopedSourceMap; // Set context to use the scoped map

            try
            {
                // --- Process FROM Clause ---
            if (querySpec.FromClause != null)
            {
                System.Diagnostics.Debug.WriteLine("[Processor] Processing FROM clause...");
                foreach (var tableRef in querySpec.FromClause.TableReferences)
                {
                    ProcessTableReference(tableRef, context); // Use helper to dispatch via factory
                }
            }
            else
            {
                 System.Diagnostics.Debug.WriteLine("[Processor] No FROM clause found.");
            }

            // --- Process WHERE Clause (Optional - Placeholder) ---
            if (querySpec.WhereClause != null)
            {
                System.Diagnostics.Debug.WriteLine("[Processor] TODO: Process WHERE clause...");
                // Could involve visiting expressions within the WHERE clause to find column references,
                // but typically doesn't directly contribute to output column lineage itself,
                // though it might influence filtering/aggregation logic not currently tracked.
                 // VisitExpression(querySpec.WhereClause.SearchCondition, context);
            }

            // --- Pre-compute Column Availability ---
            System.Diagnostics.Debug.WriteLine("[Processor] Building column availability map...");
            context.ColumnAvailabilityMap = BuildColumnAvailabilityMap(scopedSourceMap, context);
            System.Diagnostics.Debug.WriteLine($"[Processor] Column availability map built. Found {context.ColumnAvailabilityMap.Count} unique column names.");


            // --- Process SELECT Elements ---
            System.Diagnostics.Debug.WriteLine("[Processor] Processing SELECT elements...");
            var outputColumns = new List<OutputColumn>();
            if (querySpec.SelectElements != null)
            {
                foreach (var selectElement in querySpec.SelectElements)
                {
                    outputColumns.AddRange(ProcessSelectElement(selectElement, context)); // Use helper
                }
            }

             // --- Process GROUP BY / HAVING (Placeholders) ---
             if (querySpec.GroupByClause != null) { System.Diagnostics.Debug.WriteLine("[Processor] TODO: Process GROUP BY clause..."); }
             if (querySpec.HavingClause != null) { System.Diagnostics.Debug.WriteLine("[Processor] TODO: Process HAVING clause..."); }


            // --- Restore Original Source Map? ---
            // If we created a new map, should we restore the original?
            // --- Restore Original Source Map ---
            // Restore the map that was active before this processor ran.
            context.CurrentSourceMap = originalSourceMap;

            System.Diagnostics.Debug.WriteLine($"[Processor] Finished QuerySpecification. Produced {outputColumns.Count} output columns.");
            return outputColumns;
            }
            finally
            {
                 // Ensure restoration even if exceptions occur during processing
                 context.CurrentSourceMap = originalSourceMap;
                 context.ColumnAvailabilityMap = null; // Clear the availability map
            }
        }

        // Explicit implementation of base interface method
        public void Process(QuerySpecification fragment, IProcessingContext context)
        {
             ProcessQuery(fragment, context);
             // Result is ignored here. Graph updates happen via ProcessQuery -> ProcessSelectElement -> AddGraphElements.
        }

        /// <summary>
        /// Builds a map indicating which columns are potentially available from which source aliases/names
        /// in the current scope's source map.
        /// </summary>
        /// <param name="currentSourceMap">The source map for the current query scope.</param>
        /// <param name="context">The processing context (needed to resolve CTEs).</param>
        /// <returns>A dictionary where the key is a column name and the value is a list of source aliases/names providing it.</returns>
        private Dictionary<string, List<string>> BuildColumnAvailabilityMap(
            IDictionary<string, SourceInfo> currentSourceMap,
            IProcessingContext context)
        {
            var availabilityMap = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

            foreach (var kvp in currentSourceMap)
            {
                string sourceAliasOrName = kvp.Key;
                SourceInfo sourceInfo = kvp.Value;
                IEnumerable<string>? availableColumns = null;

                switch (sourceInfo.Type)
                {
                    case SourceType.Table:
                        // For tables, we don't know the exact columns without schema.
                        // However, for ambiguity detection, we need to know the table *might* provide a column.
                        // We won't add specific column names, but the ambiguity check later
                        // will need to consider tables differently if the availability map doesn't resolve the column.
                        // For now, let's just note the table exists. The ambiguity logic in SelectScalarExpressionProcessor
                        // already handles the case where the map doesn't resolve the column.
                        // No changes needed here for the map itself regarding tables.
                        availableColumns = Enumerable.Empty<string>(); // Treat as providing no known columns for the map.
                        break;
                        // continue; // Don't skip, just don't add columns to the map.

                    case SourceType.CTE:
                        if (context.TryResolveCte(sourceInfo.Name, out var cteInfo) && cteInfo != null && cteInfo.IsProcessed)
                        {
                            availableColumns = cteInfo.OutputColumnSources.Keys;
                        }
                        else
                        {
                             System.Diagnostics.Debug.WriteLine($"[Processor] Could not resolve processed CTE '{sourceInfo.Name}' for column availability map.");
                        }
                        break;

                    case SourceType.Subquery:
                        availableColumns = sourceInfo.SubqueryOutputColumns?.Select(c => c.OutputName);
                        break;
                }

                if (availableColumns != null)
                {
                    foreach (var colName in availableColumns)
                    {
                        if (!availabilityMap.TryGetValue(colName, out var sourceList))
                        {
                            sourceList = new List<string>();
                            availabilityMap[colName] = sourceList;
                        }
                        sourceList.Add(sourceAliasOrName);
                    }
                }
            }
            return availabilityMap;
        }


        // --- Helper Methods for Delegation ---

        /// <summary>
        /// Helper to process a TableReference using the factory.
        /// </summary>
        private void ProcessTableReference(TableReference tableReference, IProcessingContext context)
        {
            try
            {
                // Explicit type checking for known types to call the correct generic GetProcessor method.
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
                else if (tableReference is PivotedTableReference pivotRef)
                {
                     context.ProcessorFactory.GetProcessor(pivotRef).Process(pivotRef, context);
                }
                else if (tableReference is UnpivotedTableReference unpivotRef)
                {
                     context.ProcessorFactory.GetProcessor(unpivotRef).Process(unpivotRef, context);
                }
                else if (tableReference is VariableTableReference varRef)
                {
                     context.ProcessorFactory.GetProcessor(varRef).Process(varRef, context);
                }
                // Add other TableReference types here if needed
                else
                {
                     System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported TableReference type in FROM clause: {tableReference.GetType().Name}");
                     if (context.ProcessorFactory is ProcessorFactory pf && pf.Diagnostics != null)
                     {
                         pf.Diagnostics.ReportMissingProcessor(tableReference.GetType());
                     }
                }
            }
            catch (Exception ex)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Error processing TableReference: {ex.Message} - Type: {tableReference.GetType().Name}");
                 // Decide how to handle errors during delegation
            }
        }

        /// <summary>
        /// Helper to process a SelectElement using the factory.
        /// </summary>
        private List<OutputColumn> ProcessSelectElement(SelectElement selectElement, IProcessingContext context)
        {
             try
            {
                if (selectElement is SelectScalarExpression scalarExp)
                {
                    // Get the base processor and cast it to the specific interface
                    var processor = context.ProcessorFactory.GetProcessor(scalarExp);
                    if (processor is ISelectElementProcessor<SelectScalarExpression> elementProcessor)
                    {
                        return elementProcessor.ProcessElement(scalarExp, context);
                    }
                    else { System.Diagnostics.Debug.WriteLine($"[Processor] Error: Retrieved processor for SelectScalarExpression does not implement ISelectElementProcessor."); }
                }
                else if (selectElement is SelectStarExpression starExp)
                {
                     // Get the base processor and cast it to the specific interface
                     var processor = context.ProcessorFactory.GetProcessor(starExp);
                     if (processor is ISelectElementProcessor<SelectStarExpression> elementProcessor)
                     {
                         return elementProcessor.ProcessElement(starExp, context);
                     }
                     else { System.Diagnostics.Debug.WriteLine($"[Processor] Error: Retrieved processor for SelectStarExpression does not implement ISelectElementProcessor."); }
                }
                 // Add other SelectElement types (SelectSetVariable, etc.) if needed
                else
                {
                     System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported SelectElement type: {selectElement.GetType().Name}");
                }
            }
            catch (Exception ex)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Error processing SelectElement: {ex.Message} - Type: {selectElement.GetType().Name}");
                 // Decide how to handle errors during delegation
            }
            return new List<OutputColumn>(); // Return empty list on error or unhandled type
        }
    }
}
