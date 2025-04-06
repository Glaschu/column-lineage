using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;
using ColumnLineageCore.Helpers; // For CteInfo

namespace ColumnLineageCore.Interfaces.Processors
{
    /// <summary>
    /// Defines a contract for processing a collection of Common Table Expressions (CTEs)
    /// defined within a single WITH clause, handling dependencies and multi-pass resolution.
    /// </summary>
    public interface ICteScopeProcessor
    {
        /// <summary>
        /// Processes a collection of CTE definitions within the given context.
        /// This method implements the multi-pass logic to handle dependencies.
        /// It updates the provided context by pushing a new CTE scope containing the processed CTEs.
        /// </summary>
        /// <param name="cteDefinitions">The collection of CommonTableExpression fragments from the WITH clause.</param>
        /// <param name="context">The current processing context, which provides access to outer CTE scopes and the processor factory.</param>
        /// <returns>A dictionary containing the CteInfo objects for the CTEs successfully processed within this scope.</returns>
        /// <remarks>
        /// The caller is responsible for eventually calling PopCteScope on the context
        /// after the scope where these CTEs are relevant is processed.
        /// </remarks>
        IDictionary<string, CteInfo> ProcessCteScope(IEnumerable<CommonTableExpression> cteDefinitions, IProcessingContext context);
    }
}
