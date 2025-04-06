using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using ColumnLineageCore.Helpers;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Processes QueryDerivedTable fragments (e.g., FROM (SELECT ...) AS Derived).
    /// Processes the inner query and adds the derived table alias with its output structure to the source map.
    /// </summary>
    public class QueryDerivedTableProcessor : ITableReferenceProcessor<QueryDerivedTable>
    {
        public void Process(QueryDerivedTable derivedTable, IProcessingContext context)
        {
            if (derivedTable == null) throw new ArgumentNullException(nameof(derivedTable));
            if (context == null) throw new ArgumentNullException(nameof(context));
            if (derivedTable.Alias?.Value == null)
            {
                // Derived tables require an alias to be referenced.
                System.Diagnostics.Debug.WriteLine($"Warning: Skipping QueryDerivedTable without an alias. Fragment: {derivedTable.QueryExpression}");
                return;
            }

            string alias = derivedTable.Alias.Value;
            System.Diagnostics.Debug.WriteLine($"[Processor] Processing QueryDerivedTable: {alias}");

            // We need to process the inner QueryExpression to understand its output structure.
            // This requires getting the appropriate QueryExpressionProcessor from the factory.

            List<OutputColumn> subqueryOutputColumns;

            // --- Processing the subquery ---
            // We need to set the context flags appropriately for the subquery processing.
            // It's a subquery, and it's not (directly) defining a CTE.
            bool originalIsSubquery = context.IsSubquery;
            bool originalIsProcessingCte = context.IsProcessingCteDefinition;
            CteInfo? originalCteInfo = context.CteInfoToPopulate;

            context.IsSubquery = true;
            context.IsProcessingCteDefinition = false; // A derived table subquery doesn't define a CTE itself
            context.CteInfoToPopulate = null;

            try
            {
                // Use dynamic dispatch or type checking to get the correct processor for the specific QueryExpression type
                if (derivedTable.QueryExpression is QuerySpecification querySpec)
                {
                    var processor = context.ProcessorFactory.GetProcessor(querySpec);
                    // Assuming IQueryExpressionProcessor defines ProcessQuery
                    subqueryOutputColumns = ((IQueryExpressionProcessor<QuerySpecification>)processor).ProcessQuery(querySpec, context);
                }
                else if (derivedTable.QueryExpression is BinaryQueryExpression binaryQuery)
                {
                     var processor = context.ProcessorFactory.GetProcessor(binaryQuery);
                     subqueryOutputColumns = ((IQueryExpressionProcessor<BinaryQueryExpression>)processor).ProcessQuery(binaryQuery, context);
                }
                 else if (derivedTable.QueryExpression is QueryParenthesisExpression parenQuery)
                 {
                      // Need to handle parenthesis by processing the inner expression
                      // This might require a dedicated processor or recursive handling within this processor.
                      // For now, let's assume a simple recursive call pattern (needs refinement)
                      // This part highlights the need for a robust way to handle *any* QueryExpression.
                      // A better approach: Have a generic ProcessQueryExpression method/processor.
                      // Let's simulate that for now:
                      subqueryOutputColumns = ProcessAnyQueryExpression(derivedTable.QueryExpression, context);

                 }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported QueryExpression type in QueryDerivedTable: {derivedTable.QueryExpression.GetType().Name}");
                    subqueryOutputColumns = new List<OutputColumn>(); // Return empty list
                }
            }
            catch (Exception ex)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Error processing subquery in QueryDerivedTable '{alias}': {ex.Message}");
                 subqueryOutputColumns = new List<OutputColumn>(); // Return empty on error
                 // Consider re-throwing depending on error handling strategy
            }
            finally
            {
                 // Restore original context state
                 context.IsSubquery = originalIsSubquery;
                 context.IsProcessingCteDefinition = originalIsProcessingCte;
                 context.CteInfoToPopulate = originalCteInfo;
            }
            // --- End Processing the subquery ---

            // Add the derived table to the source map using its alias.
            // The SourceInfo contains the list of OutputColumns from the subquery,
            // which maps the subquery's output names to their *ultimate* source nodes.
            // Intermediate nodes (e.g., dt.SubVal) and edges (e.g., ScriptCTE.Val -> dt.SubVal)
            // will be created on demand by the SelectScalarExpressionProcessor when the
            // derived table columns are referenced in the outer query.
            var sourceInfo = new SourceInfo(alias, subqueryOutputColumns);
            context.CurrentSourceMap[alias] = sourceInfo;

            System.Diagnostics.Debug.WriteLine($"[Processor] Added Derived Table source '{alias}' with {subqueryOutputColumns.Count} output columns to source map.");
        }

         // Placeholder/Helper to simulate processing any QueryExpression type via factory
         // In reality, this logic might live in a central dispatcher or the factory itself.
         private List<OutputColumn> ProcessAnyQueryExpression(QueryExpression queryExpression, IProcessingContext context)
         {
              if (queryExpression is QuerySpecification querySpec)
              {
                  var processor = context.ProcessorFactory.GetProcessor(querySpec);
                  return ((IQueryExpressionProcessor<QuerySpecification>)processor).ProcessQuery(querySpec, context);
              }
              else if (queryExpression is BinaryQueryExpression binaryQuery)
              {
                  var processor = context.ProcessorFactory.GetProcessor(binaryQuery);
                  return ((IQueryExpressionProcessor<BinaryQueryExpression>)processor).ProcessQuery(binaryQuery, context);
              }
              else if (queryExpression is QueryParenthesisExpression parenQuery)
              {
                   // Recursively process the inner expression
                   return ProcessAnyQueryExpression(parenQuery.QueryExpression, context);
              }
              // Add other QueryExpression types if needed

              System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported QueryExpression type in ProcessAnyQueryExpression: {queryExpression.GetType().Name}");
              return new List<OutputColumn>();
         }
    }
}
