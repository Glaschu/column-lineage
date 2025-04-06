using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;
using ColumnLineageCore.Helpers; // For OutputColumn

namespace ColumnLineageCore.Interfaces.Processors
{
    /// <summary>
    /// Defines a contract for processors that handle specific QueryExpression types.
    /// These processors typically return the structure of the output columns produced.
    /// </summary>
    public interface IQueryExpressionProcessor<in TQueryExpression> : ISqlFragmentProcessor<TQueryExpression>
        where TQueryExpression : QueryExpression
    {
        // Overload or replace Process?
        // For query expressions, we often need to return the output structure.
        // Let's define a new method for that, keeping Process for potential side effects (like adding nodes/edges directly).

        /// <summary>
        /// Processes the query expression and returns the structure of its output columns.
        /// Implementations should also interact with the context's LineageGraph to add necessary nodes/edges.
        /// </summary>
        /// <param name="queryExpression">The query expression to process.</param>
        /// <param name="context">The current processing context.</param>
        /// <returns>A list of OutputColumn objects representing the output of this query expression.</returns>
        List<OutputColumn> ProcessQuery(TQueryExpression queryExpression, IProcessingContext context);

        // We still need the base Process method from ISqlFragmentProcessor
        // void Process(TQueryExpression fragment, IProcessingContext context);
        // Let's implement Process explicitly to call ProcessQuery, or decide if ProcessQuery replaces it.
        // For now, let's assume ProcessQuery is the primary method for these types.
        // The base Process method might not be directly used for query expressions if ProcessQuery handles graph updates.
    }
}
