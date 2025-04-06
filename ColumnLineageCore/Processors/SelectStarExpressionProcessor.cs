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
    /// Processes SelectStarExpression fragments (e.g., SELECT *, SELECT T1.*).
    /// Expands the star based on the available sources (CTEs, Subqueries) in the context.
    /// Note: Expanding '*' for base tables requires schema information, which is currently not available.
    /// </summary>
    public class SelectStarExpressionProcessor : ISelectElementProcessor<SelectStarExpression>
    {
        // Implementation of ISelectElementProcessor.ProcessElement
        public List<OutputColumn> ProcessElement(SelectStarExpression starExp, IProcessingContext context)
        {
            if (starExp == null) throw new ArgumentNullException(nameof(starExp));
            if (context == null) throw new ArgumentNullException(nameof(context));

            var outputColumns = new List<OutputColumn>();
            string? qualifier = starExp.Qualifier?.Identifiers?.LastOrDefault()?.Value;

            System.Diagnostics.Debug.WriteLine($"[Processor] Processing SelectStarExpression. Qualifier: '{qualifier ?? "None"}'");

            IEnumerable<KeyValuePair<string, SourceInfo>> sourcesToExpand;

            if (qualifier != null)
            {
                // Specific source requested (e.g., T1.*)
                if (context.CurrentSourceMap.TryGetValue(qualifier, out var sourceInfo))
                {
                    sourcesToExpand = new List<KeyValuePair<string, SourceInfo>> { new KeyValuePair<string, SourceInfo>(qualifier, sourceInfo) };
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not find source qualifier '{qualifier}' for SELECT *.");
                    sourcesToExpand = Enumerable.Empty<KeyValuePair<string, SourceInfo>>();
                }
            }
            else
            {
                // Expand all sources (SELECT *)
                sourcesToExpand = context.CurrentSourceMap;
                if (!sourcesToExpand.Any())
                {
                     System.Diagnostics.Debug.WriteLine($"[Processor] Warning: SELECT * found but no sources in FROM clause.");
                }
            }

            foreach (var kvp in sourcesToExpand)
            {
                string aliasOrName = kvp.Key; // The alias used in the FROM clause (or table/CTE name if no alias)
                SourceInfo sourceInfo = kvp.Value;

                switch (sourceInfo.Type)
                {
                    case SourceType.Table:
                        // Limitation: Cannot expand '*' for base tables without schema info.
                        System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Cannot expand SELECT * for base table '{sourceInfo.Name}' (alias '{aliasOrName}') without schema information.");
                        // Option: Create a single placeholder node? For now, do nothing.
                        break;

                    case SourceType.CTE:
                        if (context.TryResolveCte(sourceInfo.Name, out var cteInfo) && cteInfo != null && cteInfo.IsProcessed)
                        {
                            foreach (var cteOutputEntry in cteInfo.OutputColumnSources)
                            {
                                string outputName = cteOutputEntry.Key;
                                ColumnNode ultimateSourceNode = cteOutputEntry.Value;
                                ColumnNode directSourceNode = new ColumnNode(outputName, sourceInfo.Name); // Intermediate CTE node

                                AddGraphElements(outputName, ultimateSourceNode, new List<ColumnNode> { directSourceNode }, context);
                                outputColumns.Add(new OutputColumn(outputName, ultimateSourceNode));
                            }
                        }
                        else
                        {
                             System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not expand SELECT * for CTE '{sourceInfo.Name}' (alias '{aliasOrName}') because it was not found or not processed.");
                        }
                        break;

                    case SourceType.Subquery:
                        if (sourceInfo.SubqueryOutputColumns != null)
                        {
                            foreach (var subqueryCol in sourceInfo.SubqueryOutputColumns)
                            {
                                string outputName = subqueryCol.OutputName;
                                ColumnNode? ultimateSourceNode = subqueryCol.SourceNode;
                                ColumnNode directSourceNode = new ColumnNode(outputName, sourceInfo.Name); // Intermediate Subquery node

                                AddGraphElements(outputName, ultimateSourceNode, new List<ColumnNode> { directSourceNode }, context);
                                outputColumns.Add(new OutputColumn(outputName, ultimateSourceNode));
                            }
                        }
                         else
                        {
                             System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Could not expand SELECT * for Subquery '{sourceInfo.Name}' (alias '{aliasOrName}') because its output columns are missing.");
                        }
                        break;
                }
            }

            return outputColumns;
        }

        // Explicit implementation of base interface method
        public void Process(SelectStarExpression fragment, IProcessingContext context)
        {
             ProcessElement(fragment, context);
             // Result is ignored, graph updates happen within ProcessElement/AddGraphElements.
        }

        // Re-use AddGraphElements logic (could be moved to a shared utility or base class later)
        private void AddGraphElements(string outputName, ColumnNode? ultimateSourceNode, List<ColumnNode> directSourceNodes, IProcessingContext context)
        {
             // Determine the target node based on context
             ColumnNode targetNode;
             if (context.IsProcessingCteDefinition && context.CteInfoToPopulate != null)
             {
                 targetNode = new ColumnNode(outputName, context.CteInfoToPopulate.Name);
                 context.Graph.AddNode(targetNode);
                 var sourceForCteMap = ultimateSourceNode ?? targetNode;
                 context.CteInfoToPopulate.OutputColumnSources[outputName] = sourceForCteMap;
                 if(ultimateSourceNode != null && ultimateSourceNode != targetNode) context.Graph.AddNode(ultimateSourceNode);
                 System.Diagnostics.Debug.WriteLine($"[Processor] Mapped CTE output '{targetNode.Id}' to ultimate source '{sourceForCteMap.Id}'.");
             }
             else if (!context.IsSubquery)
             {
                 targetNode = new ColumnNode(outputName);
                 context.Graph.AddNode(targetNode);
                 if (ultimateSourceNode != null) context.Graph.AddNode(ultimateSourceNode);
                 System.Diagnostics.Debug.WriteLine($"[Processor] Adding final output node '{targetNode.Id}'.");
             }
             else
             {
                 if (ultimateSourceNode != null) context.Graph.AddNode(ultimateSourceNode);
                 foreach(var directSource in directSourceNodes) { context.Graph.AddNode(directSource); }
                 System.Diagnostics.Debug.WriteLine($"[Processor] Processing subquery output '{outputName}'. Ensuring sources exist.");
                 return;
             }

             // Add Edges
             if (directSourceNodes.Any())
             {
                 foreach(var directSource in directSourceNodes)
                 {
                     context.Graph.AddNode(directSource);
                     context.Graph.AddEdge(directSource, targetNode);
                     System.Diagnostics.Debug.WriteLine($"[Processor] Added edge from direct source '{directSource.Id}' to target '{targetNode.Id}'.");
                 }
             }
             // Note: No fallback to ultimate source edge here for '*' expansion, only direct sources are linked.
             else
             {
                  System.Diagnostics.Debug.WriteLine($"[Processor] No direct sources identified for '*' expansion output '{outputName}', no edge added.");
             }
        }
    }
}
