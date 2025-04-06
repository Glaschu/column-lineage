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
    /// Implements ICteScopeProcessor. Handles the multi-pass processing of CTEs within a single WITH clause.
    /// </summary>
    public class CteScopeProcessor : ICteScopeProcessor
    {
        // Define a maximum number of passes to prevent infinite loops in case of cycles or errors
        private const int MaxProcessingPasses = 10; // Adjust as needed

        public IDictionary<string, CteInfo> ProcessCteScope(IEnumerable<CommonTableExpression> cteDefinitions, IProcessingContext context)
        {
            if (cteDefinitions == null) throw new ArgumentNullException(nameof(cteDefinitions));
            if (context == null) throw new ArgumentNullException(nameof(context));

            System.Diagnostics.Debug.WriteLine($"[Processor] Starting CTE Scope Processing for {cteDefinitions.Count()} CTEs.");

            // 1. Initialize local tracking for this scope
            var localCteInfos = cteDefinitions
                .Where(cte => cte.ExpressionName?.Value != null && cte.QueryExpression != null)
                .Select(cte => new CteInfo(cte.ExpressionName!.Value, cte.QueryExpression!))
                .ToDictionary(info => info.Name, StringComparer.OrdinalIgnoreCase);

            // Keep track of the names defined in this specific scope
            var localCteNames = new HashSet<string>(localCteInfos.Keys, StringComparer.OrdinalIgnoreCase);

            var processedInThisScope = new Dictionary<string, CteInfo>(StringComparer.OrdinalIgnoreCase);

            if (!localCteInfos.Any())
            {
                 // No local CTEs to process, just push an empty scope
                 context.PushCteScope(processedInThisScope);
                 System.Diagnostics.Debug.WriteLine("[Processor] No local CTEs found in scope.");
                 return processedInThisScope;
            }

            try
            {
                // 2. Build Dependency Graph
                System.Diagnostics.Debug.WriteLine("[Processor] Building local CTE dependency graph...");
                var dependencyGraph = BuildLocalDependencyGraph(cteDefinitions, localCteNames);

                // 3. Topological Sort
                System.Diagnostics.Debug.WriteLine("[Processor] Performing topological sort...");
                var sortedCteNames = TopologicalSort(localCteInfos.Keys.ToList(), dependencyGraph);
                System.Diagnostics.Debug.WriteLine($"[Processor] Processing order: {string.Join(", ", sortedCteNames)}");

                // 4. Process CTEs in sorted order
                foreach (var cteName in sortedCteNames)
                {
                    var cteInfo = localCteInfos[cteName];
                    if (cteInfo.IsProcessed) continue; // Should not happen with correct sort, but safe check

                    System.Diagnostics.Debug.WriteLine($"Processing CTE: {cteInfo.Name}");
                    try
                    {
                        // --- Process the CTE's QueryExpression ---
                        bool originalIsProcessingCte = context.IsProcessingCteDefinition;
                        CteInfo? originalCteInfoToPopulate = context.CteInfoToPopulate;

                        context.IsProcessingCteDefinition = true;
                        context.CteInfoToPopulate = cteInfo;

                        ProcessAnyQueryExpression(cteInfo.Definition, context);

                        // --- Mark as processed ---
                        cteInfo.IsProcessed = true;
                        processedInThisScope[cteInfo.Name] = cteInfo;
                        System.Diagnostics.Debug.WriteLine($"Successfully processed CTE: {cteInfo.Name}");

                        // Restore context flags
                        context.IsProcessingCteDefinition = originalIsProcessingCte;
                        context.CteInfoToPopulate = originalCteInfoToPopulate;
                    }
                    catch (Exception ex) // Catch unexpected errors during processing
                    {
                        System.Diagnostics.Debug.WriteLine($"ERROR: Unexpected error processing CTE '{cteInfo.Name}'. Error: {ex}");
                        // Restore context flags and mark as unprocessed
                        context.IsProcessingCteDefinition = false;
                        context.CteInfoToPopulate = null;
                        cteInfo.IsProcessed = false;
                        // Optionally re-throw or collect errors
                        throw; // Re-throw for now to indicate failure
                    }
                }

                // 5. Update the main context
                context.PushCteScope(processedInThisScope);
                System.Diagnostics.Debug.WriteLine($"[Processor] Finished CTE Scope Processing. Pushed {processedInThisScope.Count} processed CTEs onto context stack.");
            }
            catch (InvalidOperationException cycleEx) // Catch cycle detection from TopologicalSort
            {
                 System.Diagnostics.Debug.WriteLine($"ERROR: {cycleEx.Message}");
                 // Push an empty scope or handle error as appropriate
                 context.PushCteScope(processedInThisScope); // Push whatever was processed before cycle detected
                 // Optionally add error information to context or result
            }
            catch (Exception ex) // Catch other unexpected errors
            {
                 System.Diagnostics.Debug.WriteLine($"ERROR: Unexpected error during CTE scope processing: {ex}");
                 context.PushCteScope(processedInThisScope); // Push partial results
                 // Optionally re-throw or collect errors
                 throw;
            }

            return processedInThisScope;
        } // End ProcessCteScope


        /// <summary>
        /// Builds a graph representing dependencies *between CTEs defined in the current scope*.
        /// </summary>
        /// <param name="cteDefinitions">The list of CTE definitions in the current scope.</param>
        /// <param name="localCteNames">A set of names for CTEs defined in the current scope.</param>
        /// <returns>A dictionary where keys are CTE names and values are sets of local CTE names they depend on.</returns>
        private Dictionary<string, HashSet<string>> BuildLocalDependencyGraph(
            IEnumerable<CommonTableExpression> cteDefinitions,
            HashSet<string> localCteNames)
        {
            var dependencyGraph = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            var dependencyFinder = new CteDependencyFinder();

            foreach (var cte in cteDefinitions)
            {
                // Ensure cte.ExpressionName is not null before accessing Value
                if (cte.ExpressionName?.Value == null) continue; // Skip CTEs without names

                string cteName = cte.ExpressionName.Value;
                dependencyGraph[cteName] = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                // Ensure cte.QueryExpression is not null before accepting visitor
                if (cte.QueryExpression == null) continue; // Skip CTEs without query expressions

                dependencyFinder.ReferencedCteNames.Clear();
                cte.QueryExpression.Accept(dependencyFinder);

                foreach (var referencedName in dependencyFinder.ReferencedCteNames)
                {
                    // Only add dependency if the referenced CTE is also defined in the *current* scope
                    if (localCteNames.Contains(referencedName))
                    {
                        dependencyGraph[cteName].Add(referencedName);
                    }
                }
            }
            return dependencyGraph;
        }

        /// <summary>
        /// Performs a topological sort on the local CTE dependency graph.
        /// Uses Kahn's algorithm.
        /// </summary>
        /// <param name="nodes">List of all CTE names in the current scope.</param>
        /// <param name="graph">The dependency graph (CTE -> Set of dependencies).</param>
        /// <returns>A list of CTE names in topological order.</returns>
        /// <exception cref="InvalidOperationException">Thrown if a cycle is detected.</exception>
        private List<string> TopologicalSort(List<string> nodes, Dictionary<string, HashSet<string>> graph)
        {
            var sortedList = new List<string>();
            var inDegree = nodes.ToDictionary(n => n, n => 0, StringComparer.OrdinalIgnoreCase);
            var adj = nodes.ToDictionary(n => n, n => new List<string>(), StringComparer.OrdinalIgnoreCase);

            // Calculate in-degrees and build adjacency list (reverse of dependency graph)
            foreach (var kvp in graph)
            {
                string dependent = kvp.Key;
                foreach (string dependency in kvp.Value)
                {
                    if (inDegree.ContainsKey(dependent)) // Ensure dependency is within the local scope graph
                    {
                         inDegree[dependent]++;
                         if (!adj.ContainsKey(dependency)) adj[dependency] = new List<string>(); // Ensure dependency key exists
                         adj[dependency].Add(dependent);
                    }
                }
            }

            // Initialize queue with nodes having in-degree 0
            var queue = new Queue<string>(nodes.Where(n => inDegree[n] == 0));

            while (queue.Count > 0)
            {
                string u = queue.Dequeue();
                sortedList.Add(u);

                if (adj.ContainsKey(u)) // Check if node u has dependents
                {
                     foreach (string v in adj[u])
                     {
                         inDegree[v]--;
                         if (inDegree[v] == 0)
                         {
                             queue.Enqueue(v);
                         }
                     }
                }
            }

            // Check for cycles
            if (sortedList.Count != nodes.Count)
            {
                var cycleNodes = nodes.Except(sortedList, StringComparer.OrdinalIgnoreCase);
                throw new InvalidOperationException($"Cyclic dependency detected among CTEs: {string.Join(", ", cycleNodes)}");
            }

            return sortedList;
        }


        // Helper to process any QueryExpression type via factory
        private List<OutputColumn> ProcessAnyQueryExpression(QueryExpression queryExpression, IProcessingContext context)
        {
              try
              {
                   if (queryExpression is QuerySpecification querySpec)
                   {
                       var processor = context.ProcessorFactory.GetProcessor(querySpec);
                       // Ensure processor implements the correct interface before casting
                       if (processor is IQueryExpressionProcessor<QuerySpecification> queryProcessor)
                       {
                           return queryProcessor.ProcessQuery(querySpec, context);
                       }
                       else
                       {
                            System.Diagnostics.Debug.WriteLine($"[Processor] Error: Processor for QuerySpecification does not implement IQueryExpressionProcessor<QuerySpecification>.");
                            return new List<OutputColumn>(); // Return empty on error
                       }
                   }
                   else if (queryExpression is BinaryQueryExpression binaryQuery)
                   {
                       var processor = context.ProcessorFactory.GetProcessor(binaryQuery);
                       // Ensure processor implements the correct interface before casting
                       if (processor is IQueryExpressionProcessor<BinaryQueryExpression> queryProcessor)
                       {
                            return queryProcessor.ProcessQuery(binaryQuery, context);
                       }
                       else
                       {
                            System.Diagnostics.Debug.WriteLine($"[Processor] Error: Processor for BinaryQueryExpression does not implement IQueryExpressionProcessor<BinaryQueryExpression>.");
                            return new List<OutputColumn>(); // Return empty on error
                       }
                   }
                   else if (queryExpression is QueryParenthesisExpression parenQuery)
                   {
                        // Ensure QueryExpression inside parenthesis is not null
                        if (parenQuery.QueryExpression != null)
                        {
                             return ProcessAnyQueryExpression(parenQuery.QueryExpression, context);
                        }
                        else
                        {
                             System.Diagnostics.Debug.WriteLine($"[Processor] Warning: QueryParenthesisExpression contains null QueryExpression.");
                             return new List<OutputColumn>();
                        }
                   }

                   System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported QueryExpression type in ProcessAnyQueryExpression: {queryExpression.GetType().Name}");
                   return new List<OutputColumn>();
              }
              catch (Exception ex) // Catch potential errors from delegated processors
              {
                   System.Diagnostics.Debug.WriteLine($"[Processor] Error during ProcessAnyQueryExpression: {ex.Message}");
                   throw; // Re-throw to signal failure up the chain
              }
         }


        // --- Helper for Dependency Checking (No longer needed by ProcessCteScope) ---

        /// <summary>
        /// [OBSOLETE - Replaced by Topological Sort] Checks if all *local* CTE dependencies for a given CTE have been processed.
        /// </summary>
        /// <param name="cteToCheck">The CTE whose dependencies are being checked.</param>
        /// <param name="context">The overall processing context (not used in this simplified check).</param>
        /// <param name="processedInCurrentScope">Dictionary of CTEs already processed in the current pass of this scope.</param>
        /// <param name="localCteNames">HashSet of all CTE names defined in this local scope.</param>
        /// <returns>True if all local dependencies are met, false otherwise.</returns>
        private bool CheckDependencies(CteInfo cteToCheck, IProcessingContext context, IDictionary<string, CteInfo> processedInCurrentScope, HashSet<string> localCteNames)
        {
            var dependencyFinder = new CteDependencyFinder();
            // Ensure Definition is not null before accepting visitor
            if (cteToCheck.Definition == null) return true; // No definition, no dependencies to check

            cteToCheck.Definition.Accept(dependencyFinder);

            foreach (var dependencyName in dependencyFinder.DependentCteNames)
            {
                // Use static string.Equals for clarity and to avoid potential compiler confusion
                if (string.Equals(dependencyName, cteToCheck.Name, StringComparison.OrdinalIgnoreCase)) continue; // Ignore self-references

                // Check if the dependency is one of the CTEs defined in *this* scope.
                if (localCteNames.Contains(dependencyName))
                {
                    // If it's a local dependency, it MUST have been processed already in this pass to proceed.
                    if (!processedInCurrentScope.ContainsKey(dependencyName))
                    {
                        System.Diagnostics.Debug.WriteLine($"Dependency Check Fail: Local CTE '{dependencyName}' required by '{cteToCheck.Name}' not processed yet in this scope/pass.");
                        return false; // Blocking local dependency not met
                    }
                }
                // Else: The dependency is either a base table or an outer-scope CTE.
                // We don't block processing based on these; resolution happens later.
            }
            return true; // All blocking (local, unprocessed) dependencies are met
        }


        /// <summary>
        /// Simple visitor to find named table references (potential CTE dependencies).
        /// </summary>
        private class CteDependencyFinder : TSqlFragmentVisitor
        {
            // Renamed for clarity to match usage in BuildLocalDependencyGraph
            public HashSet<string> ReferencedCteNames { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            // Keep DependentCteNames for CheckDependencies compatibility for now
            public HashSet<string> DependentCteNames => ReferencedCteNames;


            public override void Visit(NamedTableReference node)
            {
                // Only consider base identifier as potential CTE name
                if (node.SchemaObject?.BaseIdentifier?.Value != null)
                {
                    ReferencedCteNames.Add(node.SchemaObject.BaseIdentifier.Value);
                }
                base.Visit(node);
            }
        }

    } // End CteScopeProcessor class


    /// <summary>
    /// Custom exception to signal that a CTE dependency was not yet processed.
    /// (Note: This exception is no longer thrown by CteScopeProcessor itself, but might be useful elsewhere or if pre-check logic changes)
    /// </summary>
    public class DependencyNotMetException : InvalidOperationException
    {
        public string DependencyName { get; }

        public DependencyNotMetException(string dependencyName)
            : base($"Dependency CTE '{dependencyName}' not processed yet.")
        {
            DependencyName = dependencyName;
        }
         public DependencyNotMetException(string dependencyName, string message) : base(message)
         {
              DependencyName = dependencyName;
         }
         public DependencyNotMetException(string dependencyName, string message, Exception inner) : base(message, inner)
         {
              DependencyName = dependencyName;
         }
    }
}
