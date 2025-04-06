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
    /// Processes BinaryQueryExpression fragments (UNION, INTERSECT, EXCEPT).
    /// Processes both sides of the expression and combines the lineage.
    /// </summary>
    public class BinaryQueryExpressionProcessor : IQueryExpressionProcessor<BinaryQueryExpression>
    {
        public List<OutputColumn> ProcessQuery(BinaryQueryExpression binaryQuery, IProcessingContext context)
        {
            if (binaryQuery == null) throw new ArgumentNullException(nameof(binaryQuery));
            if (context == null) throw new ArgumentNullException(nameof(context));

            System.Diagnostics.Debug.WriteLine($"[Processor] Processing BinaryQueryExpression: {binaryQuery.BinaryQueryExpressionType}");

            // --- Process Both Sides Recursively ---
            // Set context flags: these are subqueries relative to the binary operation itself.
            bool originalIsSubquery = context.IsSubquery;
            bool originalIsProcessingCte = context.IsProcessingCteDefinition;
            CteInfo? originalCteInfo = context.CteInfoToPopulate;

            context.IsSubquery = true; // Both sides are treated as subqueries
            context.IsProcessingCteDefinition = false; // The binary operation itself doesn't define a CTE
            context.CteInfoToPopulate = null;

            List<OutputColumn> firstOutput = new List<OutputColumn>();
            List<OutputColumn> secondOutput = new List<OutputColumn>();

            try
            {
                firstOutput = ProcessAnyQueryExpression(binaryQuery.FirstQueryExpression, context);
                secondOutput = ProcessAnyQueryExpression(binaryQuery.SecondQueryExpression, context);
            }
            finally
            {
                 // Restore original context state
                 context.IsSubquery = originalIsSubquery;
                 context.IsProcessingCteDefinition = originalIsProcessingCte;
                 context.CteInfoToPopulate = originalCteInfo;
            }

            // --- Combine Results and Add Graph Elements ---
            var combinedOutput = new List<OutputColumn>();
            int columnCount = Math.Min(firstOutput.Count, secondOutput.Count); // Column count must match for UNION etc.

            if (firstOutput.Count != secondOutput.Count)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Column count mismatch in {binaryQuery.BinaryQueryExpressionType}. Left: {firstOutput.Count}, Right: {secondOutput.Count}. Using minimum count ({columnCount}).");
            }

            for (int i = 0; i < columnCount; i++)
            {
                // Output name is determined by the first query's output column name at this position.
                string outputName = firstOutput[i].OutputName;
                ColumnNode? source1 = firstOutput[i].SourceNode;
                ColumnNode? source2 = secondOutput[i].SourceNode;

                // Add graph elements only if this binary query is NOT part of a larger subquery or CTE definition.
                if (!context.IsSubquery && !context.IsProcessingCteDefinition)
                {
                    var targetNode = new ColumnNode(outputName); // Final output node
                    context.Graph.AddNode(targetNode);

                    // Add edges from the ultimate sources identified by the recursive calls
                    if (source1 != null)
                    {
                        context.Graph.AddNode(source1); // Ensure source exists
                        context.Graph.AddEdge(source1, targetNode);
                        System.Diagnostics.Debug.WriteLine($"[Processor] Added edge from binary source1 '{source1.Id}' to target '{targetNode.Id}'.");
                    }
                     if (source2 != null)
                    {
                        context.Graph.AddNode(source2); // Ensure source exists
                        context.Graph.AddEdge(source2, targetNode);
                         System.Diagnostics.Debug.WriteLine($"[Processor] Added edge from binary source2 '{source2.Id}' to target '{targetNode.Id}'.");
                    }
                }
                else if (context.IsProcessingCteDefinition && context.CteInfoToPopulate != null)
                {
                     // Populate the CTE info being defined. Target is the intermediate CTE node.
                     var targetNode = new ColumnNode(outputName, context.CteInfoToPopulate.Name);
                     context.Graph.AddNode(targetNode);

                     // Map to *one* of the sources (e.g., source1) for the CTE definition.
                     // Handling multiple sources feeding one CTE output via UNION is complex.
                     // Simplification: Map to source1 if available, otherwise map to self (intermediate).
                     var sourceForCteMap = source1 ?? targetNode;
                     context.CteInfoToPopulate.OutputColumnSources[outputName] = sourceForCteMap;
                     if(source1 != null && source1 != targetNode) context.Graph.AddNode(source1); // Ensure ultimate source exists
                     if(source2 != null) context.Graph.AddNode(source2); // Ensure other source also exists

                     System.Diagnostics.Debug.WriteLine($"[Processor] Mapped CTE output '{targetNode.Id}' from binary op to ultimate source '{sourceForCteMap.Id}'.");
                }
                 else // Is part of a subquery
                 {
                      // Ensure ultimate source nodes exist, but don't add final target or edges.
                      if (source1 != null) context.Graph.AddNode(source1);
                      if (source2 != null) context.Graph.AddNode(source2);
                      System.Diagnostics.Debug.WriteLine($"[Processor] Processing binary op subquery output '{outputName}'. Ensuring sources exist.");
                 }


                // Represent the combined output structure for return value.
                // Use source1 as the representative source for the structure.
                combinedOutput.Add(new OutputColumn(outputName, source1));
            }

            System.Diagnostics.Debug.WriteLine($"[Processor] Finished BinaryQueryExpression. Combined {combinedOutput.Count} output columns.");
            return combinedOutput;
        }

        // Explicit implementation of base interface method
        public void Process(BinaryQueryExpression fragment, IProcessingContext context)
        {
             ProcessQuery(fragment, context);
             // Result is ignored here.
        }

         // Placeholder/Helper to simulate processing any QueryExpression type via factory
         // Copied from QueryDerivedTableProcessor - consider moving to a shared utility/base class.
         private List<OutputColumn> ProcessAnyQueryExpression(QueryExpression queryExpression, IProcessingContext context)
         {
              if (queryExpression is QuerySpecification querySpec)
              {
                  var processor = context.ProcessorFactory.GetProcessor(querySpec);
                  // Cast needed if ProcessQuery isn't on the base ISqlFragmentProcessor
                  return ((IQueryExpressionProcessor<QuerySpecification>)processor).ProcessQuery(querySpec, context);
              }
              else if (queryExpression is BinaryQueryExpression binaryQuery)
              {
                  // Recursive call for nested binary expressions
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
