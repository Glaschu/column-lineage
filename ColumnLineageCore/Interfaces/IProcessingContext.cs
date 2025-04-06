using ColumnLineageCore.Model; // For ColumnNode
using ColumnLineageCore.Helpers; // For CteInfo, SourceInfo - Moved after Model to check if order matters
using System.Collections.Generic;

namespace ColumnLineageCore.Interfaces
{
    /// <summary>
    /// Defines the contract for managing the shared state during SQL fragment processing.
    /// This includes the lineage graph, CTE context, and current source information.
    /// </summary>
    public interface IProcessingContext
    {
        /// <summary>
        /// Gets the lineage graph being built.
        /// </summary>
        ILineageGraph Graph { get; }

        /// <summary>
        /// Gets the factory for retrieving fragment processors.
        /// </summary>
        IProcessorFactory ProcessorFactory { get; }

        /// <summary>
        /// Pushes a new CTE scope onto the context stack.
        /// Used when entering a WITH clause.
        /// </summary>
        /// <param name="ctesInScope">The CTEs defined in this new scope.</param>
        void PushCteScope(IDictionary<string, CteInfo> ctesInScope);

        /// <summary>
        /// Pops the current CTE scope from the context stack.
        /// Used when leaving a WITH clause or the statement it belongs to.
        /// </summary>
        void PopCteScope();

        /// <summary>
        /// Attempts to resolve a CTE name within the current scope hierarchy.
        /// Searches from the innermost scope outwards.
        /// </summary>
        /// <param name="cteName">The name of the CTE to resolve.</param>
        /// <param name="cteInfo">The resolved CteInfo if found; otherwise, null.</param>
        /// <returns>True if the CTE was found in the current context, false otherwise.</returns>
        bool TryResolveCte(string cteName, out CteInfo? cteInfo);

        /// <summary>
        /// Gets the current source map (alias/name to SourceInfo) for the query being processed.
        /// This might be managed per QuerySpecification scope.
        /// </summary>
        /// <remarks>
        /// The exact management (stack vs. single level) might need refinement.
        /// </remarks>
        IDictionary<string, SourceInfo> CurrentSourceMap { get; set; } // Consider if set is appropriate or if methods are better

        /// <summary>
        /// Gets or sets a flag indicating if the current processing is for a CTE definition.
        /// This helps processors know whether to create final output nodes or intermediate CTE nodes.
        /// </summary>
        bool IsProcessingCteDefinition { get; set; }

        /// <summary>
        /// Gets or sets the CteInfo object to be populated when IsProcessingCteDefinition is true.
        /// </summary>
        CteInfo? CteInfoToPopulate { get; set; }

        /// <summary>
        /// Gets or sets a flag indicating if the current processing is part of a subquery
        /// (e.g., a side of a UNION, a derived table) where final output nodes shouldn't be created directly.
        /// </summary>
        bool IsSubquery { get; set; }

        /// <summary>
        /// Gets or sets the pre-computed map of column availability for the current query scope.
        /// Key: Column Name, Value: List of source aliases/names providing that column.
        /// This is typically built by QuerySpecificationProcessor before processing SELECT elements.
        /// </summary>
        Dictionary<string, List<string>>? ColumnAvailabilityMap { get; set; }

        /// <summary>
        /// Gets or sets the target table name if the current statement is a SELECT INTO.
        /// Null otherwise.
        /// </summary>
        string? IntoClauseTarget { get; set; }

        /// <summary>
        /// Gets the provider for view/procedure definitions.
        /// </summary>
        IViewDefinitionProvider ViewProvider { get; } // Added

        /// <summary>
        /// Gets the cache for storing analysis results of views/procedures.
        /// Key: Object Name, Value: LineageResult
        /// </summary>
        IDictionary<string, LineageResult> AnalysisCache { get; } // Added

        /// <summary>
        /// Registers the identified output columns for a specific procedure.
        /// This allows subsequent statements (like INSERT EXEC) to retrieve them.
        /// </summary>
        /// <param name="procedureName">The fully qualified name of the procedure.</param>
        /// <param name="outputColumns">The list of output columns determined from the procedure's definition.</param>
        void RegisterProcedureOutput(string procedureName, List<OutputColumn> outputColumns);

        /// <summary>
        /// Attempts to retrieve the previously registered output columns for a procedure.
        /// </summary>
        /// <param name="procedureName">The fully qualified name of the procedure.</param>
        /// <param name="outputColumns">The list of output columns if found; otherwise, null.</param>
        /// <returns>True if output columns were found for the procedure, false otherwise.</returns>
        bool TryGetProcedureOutput(string procedureName, out List<OutputColumn>? outputColumns);


        // Consider adding methods for managing source map scope if needed (Push/PopSourceMap)
    }
}
