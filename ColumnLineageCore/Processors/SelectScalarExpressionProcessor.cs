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
    /// Processes SelectScalarExpression fragments (e.g., SELECT ColA, t1.ColB, MAX(ColC) AS MaxC, 'Literal').
    /// Determines the output column name, resolves sources, adds nodes/edges to the graph,
    /// and returns the OutputColumn structure.
    /// </summary>
    public class SelectScalarExpressionProcessor : ISelectElementProcessor<SelectScalarExpression>
    {
        // Implementation of ISelectElementProcessor.ProcessElement
        public List<OutputColumn> ProcessElement(SelectScalarExpression selectScalar, IProcessingContext context)
        {
            if (selectScalar == null) throw new ArgumentNullException(nameof(selectScalar));
            if (context == null) throw new ArgumentNullException(nameof(context));

            string? outputName = DetermineOutputName(selectScalar);

            if (string.IsNullOrWhiteSpace(outputName))
            {
                System.Diagnostics.Debug.WriteLine($"[Processor] Skipping SelectScalarExpression without a determinable output name. Expression: {selectScalar.Expression.GetType().Name}");
                return new List<OutputColumn>(); // Return empty list
            }

            System.Diagnostics.Debug.WriteLine($"[Processor] Processing SelectScalarExpression: Output Name = {outputName}");

            ColumnNode? ultimateSourceNode = null;
            List<ColumnNode> directSourceNodes = new List<ColumnNode>();

            if (selectScalar.Expression is ColumnReferenceExpression colRef)
            {
                ResolveColumnReferenceSource(colRef, context, outputName, out ultimateSourceNode, out var directSource);
                if (directSource != null) directSourceNodes.Add(directSource);
            }
            else if (selectScalar.Expression is FunctionCall funcCall)
            {
                 ultimateSourceNode = null; // Function output source is considered untraceable for now
                 System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing FunctionCall: {funcCall.FunctionName.Value}");

                 // Use ColumnReferenceFinder to find columns within parameters and OverClause recursively
                 var columnFinder = new ColumnReferenceFinder();
                 if (funcCall.Parameters != null)
                 {
                      foreach (var paramExpr in funcCall.Parameters)
                      {
                           paramExpr?.Accept(columnFinder); // Visit the parameter expression
                      }
                 }
                 funcCall.OverClause?.Accept(columnFinder); // Also check OverClause expressions recursively

                 // Resolve sources for all found column references
                 foreach(var foundColRef in columnFinder.ColumnReferences)
                 {
                      ResolveColumnReferenceSource(foundColRef, context, outputName + "_funcInput", out _, out var directSource);
                      if (directSource != null && !directSourceNodes.Contains(directSource))
                      {
                           directSourceNodes.Add(directSource);
                           System.Diagnostics.Debug.WriteLine($"[Processor] Added function input source: {directSource.Id}");
                      }
                 }
            }
            else if (selectScalar.Expression is BinaryExpression binExpr)
            {
                 ultimateSourceNode = null;
                 System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing BinaryExpression ({binExpr.BinaryExpressionType})...");
                 var columnFinder = new ColumnReferenceFinder();
                 binExpr.FirstExpression?.Accept(columnFinder);
                 binExpr.SecondExpression?.Accept(columnFinder);
                 foreach(var foundColRef in columnFinder.ColumnReferences)
                 {
                      ResolveColumnReferenceSource(foundColRef, context, outputName + "_binaryInput", out _, out var directSource);
                      if (directSource != null && !directSourceNodes.Contains(directSource))
                      {
                           directSourceNodes.Add(directSource);
                           System.Diagnostics.Debug.WriteLine($"[Processor] Added binary expression input source: {directSource.Id}");
                      }
                 }
            }
            else if (selectScalar.Expression is Literal)
            {
                 ultimateSourceNode = null;
            }
            else if (selectScalar.Expression is ScalarSubquery) // Added basic handling
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Encountered ScalarSubquery for output '{outputName}'. Treating as untraceable source.");
                 ultimateSourceNode = null;
                 // TODO: Optionally process the subquery itself to find its sources, though linking them here is complex.
            }
            else if (selectScalar.Expression is SearchedCaseExpression caseExpr)
            {
                 ultimateSourceNode = null; // CASE expression source is untraceable for now
                 System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing SearchedCaseExpression...");
                 var columnFinder = new ColumnReferenceFinder();
                 // Visit WHEN conditions and THEN results
                 foreach(var whenClause in caseExpr.WhenClauses)
                 {
                      whenClause.WhenExpression?.Accept(columnFinder);
                      whenClause.ThenExpression?.Accept(columnFinder);
                 }
                 // Visit ELSE result
                 caseExpr.ElseExpression?.Accept(columnFinder);

                 // Resolve sources for all found column references
                 foreach(var foundColRef in columnFinder.ColumnReferences)
                 {
                      ResolveColumnReferenceSource(foundColRef, context, outputName + "_caseInput", out _, out var directSource);
                      if (directSource != null && !directSourceNodes.Contains(directSource))
                      {
                           directSourceNodes.Add(directSource);
                           System.Diagnostics.Debug.WriteLine($"[Processor] Added CASE expression input source: {directSource.Id}");
                      }
                 }
            }
            else if (selectScalar.Expression is SimpleCaseExpression simpleCaseExpr)
            {
                ultimateSourceNode = null; // Simple CASE expression source is untraceable for now
                System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing SimpleCaseExpression...");
                var columnFinder = new ColumnReferenceFinder();
                
                // Visit the input expression (the expression being evaluated)
                simpleCaseExpr.InputExpression?.Accept(columnFinder);
                
                // Visit WHEN values and THEN results
                foreach(var whenClause in simpleCaseExpr.WhenClauses)
                {
                     whenClause.WhenExpression?.Accept(columnFinder);
                     whenClause.ThenExpression?.Accept(columnFinder);
                }
                // Visit ELSE result
                simpleCaseExpr.ElseExpression?.Accept(columnFinder);

                // Resolve sources for all found column references
                foreach(var foundColRef in columnFinder.ColumnReferences)
                {
                     ResolveColumnReferenceSource(foundColRef, context, outputName + "_simpleCaseInput", out _, out var directSource);
                     if (directSource != null && !directSourceNodes.Contains(directSource))
                     {
                          directSourceNodes.Add(directSource);
                          System.Diagnostics.Debug.WriteLine($"[Processor] Added simple CASE expression input source: {directSource.Id}");
                     }
                }
            }
            else if (selectScalar.Expression is CastCall castExpr)
            {
                ultimateSourceNode = null; // CAST expression preserves source but changes type
                System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing CastCall expression...");
                var columnFinder = new ColumnReferenceFinder();
                
                // Visit the parameter being cast
                castExpr.Parameter?.Accept(columnFinder);

                // Resolve sources for all found column references
                foreach(var foundColRef in columnFinder.ColumnReferences)
                {
                     ResolveColumnReferenceSource(foundColRef, context, outputName + "_castInput", out _, out var directSource);
                     if (directSource != null && !directSourceNodes.Contains(directSource))
                     {
                          directSourceNodes.Add(directSource);
                          System.Diagnostics.Debug.WriteLine($"[Processor] Added CAST expression input source: {directSource.Id}");
                     }
                }
            }
            else if (selectScalar.Expression is ConvertCall convertExpr)
            {
                ultimateSourceNode = null; // CONVERT expression preserves source but changes type
                System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing ConvertCall expression...");
                var columnFinder = new ColumnReferenceFinder();
                
                // Visit the parameter being converted (ConvertCall typically has Parameter property)
                convertExpr.Parameter?.Accept(columnFinder);

                // Resolve sources for all found column references
                foreach(var foundColRef in columnFinder.ColumnReferences)
                {
                     ResolveColumnReferenceSource(foundColRef, context, outputName + "_convertInput", out _, out var directSource);
                     if (directSource != null && !directSourceNodes.Contains(directSource))
                     {
                          directSourceNodes.Add(directSource);
                          System.Diagnostics.Debug.WriteLine($"[Processor] Added CONVERT expression input source: {directSource.Id}");
                     }
                }
            }
            else if (selectScalar.Expression is FunctionCall functionCall)
            {
                ultimateSourceNode = null; // Function calls typically transform input columns
                System.Diagnostics.Debug.WriteLine($"[Processor] Analyzing FunctionCall: {functionCall.FunctionName?.Value ?? "Unknown"}");
                var columnFinder = new ColumnReferenceFinder();
                
                // Visit all function parameters
                foreach(var param in functionCall.Parameters)
                {
                     param?.Accept(columnFinder);
                }

                // Resolve sources for all found column references
                foreach(var foundColRef in columnFinder.ColumnReferences)
                {
                     ResolveColumnReferenceSource(foundColRef, context, outputName + "_funcInput", out _, out var directSource);
                     if (directSource != null && !directSourceNodes.Contains(directSource))
                     {
                          directSourceNodes.Add(directSource);
                          System.Diagnostics.Debug.WriteLine($"[Processor] Added function call input source: {directSource.Id}");
                     }
                }
            }
            else if (selectScalar.Expression is ParenthesisExpression parenExpr)
            {
                // Handle expressions wrapped in parentheses
                System.Diagnostics.Debug.WriteLine($"[Processor] Processing ParenthesisExpression...");
                if (parenExpr.Expression is ColumnReferenceExpression nestedColRef)
                {
                    ResolveColumnReferenceSource(nestedColRef, context, outputName, out ultimateSourceNode, out var directSource);
                    if (directSource != null && !directSourceNodes.Contains(directSource))
                    {
                        directSourceNodes.Add(directSource);
                    }
                }
                else
                {
                    ultimateSourceNode = null;
                    var columnFinder = new ColumnReferenceFinder();
                    parenExpr.Expression?.Accept(columnFinder);
                    foreach(var foundColRef in columnFinder.ColumnReferences)
                    {
                        ResolveColumnReferenceSource(foundColRef, context, outputName + "_parenInput", out _, out var directSource);
                        if (directSource != null && !directSourceNodes.Contains(directSource))
                        {
                            directSourceNodes.Add(directSource);
                            System.Diagnostics.Debug.WriteLine($"[Processor] Added parenthesis expression input source: {directSource.Id}");
                        }
                    }
                }
            }
            // TODO: Add handlers for other complex expressions as needed
            else
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unhandled expression type in SelectScalarExpression: {selectScalar.Expression.GetType().Name}");
                 ultimateSourceNode = null;
                 
                 // Try to find any column references in unhandled expressions
                 var columnFinder = new ColumnReferenceFinder();
                 selectScalar.Expression?.Accept(columnFinder);
                 foreach(var foundColRef in columnFinder.ColumnReferences)
                 {
                     ResolveColumnReferenceSource(foundColRef, context, outputName + "_unknownInput", out _, out var directSource);
                     if (directSource != null && !directSourceNodes.Contains(directSource))
                     {
                         directSourceNodes.Add(directSource);
                         System.Diagnostics.Debug.WriteLine($"[Processor] Added unknown expression input source: {directSource.Id}");
                     }
                 }
            }

            // --- Add Nodes and Edges ---
            bool resolutionAttempted = selectScalar.Expression is ColumnReferenceExpression;
            bool resolutionSucceeded = ultimateSourceNode != null || directSourceNodes.Any();

            // For complex expressions with a single source, promote the direct source to ultimate source
            if (ultimateSourceNode == null && directSourceNodes.Count == 1)
            {
                ultimateSourceNode = directSourceNodes[0];
                directSourceNodes.Clear(); // Clear since we're promoting it to ultimate
                System.Diagnostics.Debug.WriteLine($"[Processor] Promoted single direct source to ultimate source for '{outputName}': {ultimateSourceNode.Id}");
            }

            // Call AddGraphElements UNLESS it was a ColumnReference AND resolution failed.
            // This ensures target nodes are created for literals, functions, etc.
            if (!resolutionAttempted || resolutionSucceeded)
            {
                 AddGraphElements(outputName, ultimateSourceNode, directSourceNodes, context);
            }
            else // It was a ColumnReference, but resolution failed
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Skipping AddGraphElements for '{outputName}' due to failed source resolution for ColumnReference.");
            }


            // --- Return Output Structure ---
            // Return OutputColumn UNLESS it was a ColumnReference AND resolution failed.
            if (resolutionAttempted && !resolutionSucceeded)
            {
                return new List<OutputColumn>();
            }
            else
            {
                // For non-ColumnReference expressions or successful ColumnReference resolution,
                // return the output column structure (ultimateSourceNode might be null).
                var outputColumn = new OutputColumn(outputName, ultimateSourceNode);
                return new List<OutputColumn> { outputColumn };
             }
        }

        public void Process(SelectScalarExpression fragment, IProcessingContext context)
        {
             ProcessElement(fragment, context);
        }

        private string? DetermineOutputName(SelectScalarExpression selectScalar)
        {
            if (selectScalar.ColumnName?.Value != null)
            {
                return selectScalar.ColumnName.Value;
            }
            if (selectScalar.Expression is ColumnReferenceExpression colRef)
            {
                return colRef.MultiPartIdentifier?.Identifiers?.LastOrDefault()?.Value;
            }
            return null;
        }

        // Made internal static to be reusable by other processors
        internal static void ResolveColumnReferenceSource(ColumnReferenceExpression colRef, IProcessingContext context, string outputName, out ColumnNode? ultimateSourceNode, out ColumnNode? directSourceNode)
        {
            ultimateSourceNode = null;
            directSourceNode = null;
            var parts = colRef.MultiPartIdentifier?.Identifiers;
            if (parts == null || !parts.Any()) return;
            string sourceColumnName = parts.Last().Value;
            string? sourceIdentifier = parts.Count > 1 ? parts[parts.Count - 2].Value : null;
            SourceInfo? sourceInfo = null;
            string? resolvedSourceIdentifier = null;

            if (sourceIdentifier != null)
            {
                var sourceMapDict = (IDictionary<string, SourceInfo>)context.CurrentSourceMap;
                if (sourceMapDict.TryGetValue(sourceIdentifier, out sourceInfo))
                {
                    resolvedSourceIdentifier = sourceIdentifier;
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not resolve source identifier '{sourceIdentifier}' for column '{sourceColumnName}' in output '{outputName}'.");
                    return;
                }
            }
            else if (context.CurrentSourceMap.Count == 1)
            {
                var kvp = context.CurrentSourceMap.First();
                sourceInfo = kvp.Value;
                resolvedSourceIdentifier = kvp.Key;
            }
            else if (context.CurrentSourceMap.Count > 1)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Resolving unqualified column '{sourceColumnName}' in output '{outputName}'. Sources: {string.Join(", ", context.CurrentSourceMap.Keys)}");
                 if (context.ColumnAvailabilityMap != null && context.ColumnAvailabilityMap.TryGetValue(sourceColumnName, out var providingSources))
                 {
                      if (providingSources.Count == 1)
                      {
                           resolvedSourceIdentifier = providingSources[0];
                           sourceInfo = context.CurrentSourceMap[resolvedSourceIdentifier];
                           System.Diagnostics.Debug.WriteLine($"[Processor] Unqualified column '{sourceColumnName}' resolved to single source '{resolvedSourceIdentifier}'.");
                      }
                      else if (providingSources.Count > 1)
                      {
                           System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Ambiguous unqualified column '{sourceColumnName}'. Provided by sources: {string.Join(", ", providingSources)}.");
                           return;
                      }
                      else
                      {
                           System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Column '{sourceColumnName}' found in availability map but source list is empty.");
                           return;
                      }
                 }
                 else
                 {
                      var tableSources = context.CurrentSourceMap.Where(kvp => kvp.Value.Type == SourceType.Table).ToList();
                      if (tableSources.Count == 1)
                      {
                           resolvedSourceIdentifier = tableSources[0].Key;
                           sourceInfo = tableSources[0].Value;
                           System.Diagnostics.Debug.WriteLine($"[Processor] Unqualified column '{sourceColumnName}' resolved to single table source '{resolvedSourceIdentifier}' (not found in availability map).");
                      }
                      else if (tableSources.Count > 1)
                      {
                           System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Ambiguous unqualified column '{sourceColumnName}'. Not found in availability map, multiple table sources exist: {string.Join(", ", tableSources.Select(t => t.Key))}.");
                           return;
                      }
                      else
                      {
                           System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unqualified column '{sourceColumnName}' not found in availability map and no table sources exist.");
                           return;
                      }
                 }
            }
            else
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Column reference '{sourceColumnName}' found but no sources in FROM clause for output '{outputName}'.");
                 return;
            }

            if (sourceInfo != null && resolvedSourceIdentifier != null)
            {
                switch (sourceInfo.Type)
                {
                    case SourceType.Table:
                        ultimateSourceNode = new ColumnNode(sourceColumnName, sourceInfo.Name);
                        directSourceNode = ultimateSourceNode;
                        break;
                    case SourceType.CTE:
                        directSourceNode = new ColumnNode(sourceColumnName, sourceInfo.Name);
                        if (context.TryResolveCte(sourceInfo.Name, out var cteInfo) && cteInfo != null && cteInfo.IsProcessed)
                        {
                            if (!cteInfo.OutputColumnSources.TryGetValue(sourceColumnName, out ultimateSourceNode))
                            { System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Column '{sourceColumnName}' not found in processed CTE '{sourceInfo.Name}' output for output '{outputName}'."); }
                        }
                        else { System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not resolve or find processed CTE '{sourceInfo.Name}' for column '{sourceColumnName}' in output '{outputName}'."); }
                        break;
                    case SourceType.Subquery:
                        var subqueryOutputCol = sourceInfo.SubqueryOutputColumns?.FirstOrDefault(col => col.OutputName.Equals(sourceColumnName, StringComparison.OrdinalIgnoreCase));
                        if (subqueryOutputCol != null)
                        {
                            ultimateSourceNode = subqueryOutputCol.SourceNode;
                            directSourceNode = new ColumnNode(sourceColumnName, sourceInfo.Name);
                            context.Graph.AddNode(directSourceNode);
                            if (ultimateSourceNode != null)
                            {
                                context.Graph.AddNode(ultimateSourceNode);
                                context.Graph.AddEdge(ultimateSourceNode, directSourceNode);
                                System.Diagnostics.Debug.WriteLine($"[Processor] Added edge from ultimate source '{ultimateSourceNode.Id}' to subquery intermediate '{directSourceNode.Id}' on demand.");
                            }
                            else { System.Diagnostics.Debug.WriteLine($"[Processor] Added subquery intermediate node '{directSourceNode.Id}' on demand (no traceable ultimate source)."); }
                        }
                        else { System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Column '{sourceColumnName}' not found in subquery '{sourceInfo.Name}' output for output '{outputName}'."); }
                        break;
                }
            }
        }

        // [OBSOLETE - Replaced by BuildColumnAvailabilityMap]
        private bool SourceProvidesColumn(SourceInfo sourceInfo, string columnName, IProcessingContext context)
        {
             return false;
        }

        private void AddGraphElements(string outputName, ColumnNode? ultimateSourceNode, List<ColumnNode> directSourceNodes, IProcessingContext context)
        {
             ColumnNode targetNode;
             if (!string.IsNullOrEmpty(context.IntoClauseTarget))
             {
                 targetNode = new ColumnNode(outputName, context.IntoClauseTarget);
                 System.Diagnostics.Debug.WriteLine($"[Processor] Target node is SELECT INTO target: '{targetNode.Id}'.");
             }
             else if (context.IsProcessingCteDefinition && context.CteInfoToPopulate != null)
             {
                 targetNode = new ColumnNode(outputName, context.CteInfoToPopulate.Name);
                 System.Diagnostics.Debug.WriteLine($"[Processor] Target node is CTE definition intermediate: '{targetNode.Id}'.");
                 var sourceForCteMap = ultimateSourceNode ?? targetNode;
                 context.CteInfoToPopulate.OutputColumnSources[outputName] = sourceForCteMap;
                 if(ultimateSourceNode != null && ultimateSourceNode != targetNode) context.Graph.AddNode(ultimateSourceNode);
                 System.Diagnostics.Debug.WriteLine($"[Processor] Mapped CTE output '{targetNode.Id}' to ultimate source '{sourceForCteMap.Id}'.");
             }
             else if (!context.IsSubquery)
             {
                 targetNode = new ColumnNode(outputName);
                 System.Diagnostics.Debug.WriteLine($"[Processor] Target node is final output: '{targetNode.Id}'.");
                 if (ultimateSourceNode != null) context.Graph.AddNode(ultimateSourceNode);
             }
             else
             {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Processing subquery output '{outputName}'. Deferring node/edge creation.");
                 if (ultimateSourceNode != null) context.Graph.AddNode(ultimateSourceNode);
                 foreach(var directSource in directSourceNodes) { context.Graph.AddNode(directSource); }
                 System.Diagnostics.Debug.WriteLine($"[Processor] Processing subquery output '{outputName}'. Deferring edge creation, ensuring source nodes exist.");
                 return;
             }

             context.Graph.AddNode(targetNode);
             if (ultimateSourceNode != null && ultimateSourceNode != targetNode) context.Graph.AddNode(ultimateSourceNode);

             if (directSourceNodes.Any())
             {
                 foreach(var directSource in directSourceNodes)
                 {
                     context.Graph.AddNode(directSource);
                     context.Graph.AddEdge(directSource, targetNode);
                     System.Diagnostics.Debug.WriteLine($"[Processor] Added edge from direct source '{directSource.Id}' to target '{targetNode.Id}'.");
                 }
             }
             else if (ultimateSourceNode != null)
             {
                 context.Graph.AddEdge(ultimateSourceNode, targetNode);
                 System.Diagnostics.Debug.WriteLine($"[Processor] Added edge from ultimate source '{ultimateSourceNode.Id}' to target '{targetNode.Id}'.");
             }
             else
             {
                  System.Diagnostics.Debug.WriteLine($"[Processor] No traceable source for output '{outputName}', no edge added.");
             }
        }
    }
}
