using Microsoft.SqlServer.TransactSql.ScriptDom;
using ColumnLineageCore.Helpers; // For OutputColumn
using System.Collections.Generic; // For List (if needed later)

namespace ColumnLineageCore.Interfaces.Processors
{
    /// <summary>
    /// Defines a contract for processors that handle specific SelectElement types (e.g., SelectScalarExpression, SelectStarExpression).
    /// These processors determine the output column structure and link to sources.
    /// </summary>
    public interface ISelectElementProcessor<in TSelectElement> : ISqlFragmentProcessor<TSelectElement>
        where TSelectElement : SelectElement
    {
        // Similar to QueryExpression, processing a select element often results in output structure.
        // However, a single SelectElement might produce multiple OutputColumns (e.g., SelectStar).
        // Let's define a method that returns a list of OutputColumns.

        /// <summary>
        /// Processes the select element and returns the corresponding output column(s) structure.
        /// Implementations should resolve sources using the context's source map and potentially interact with the graph.
        /// </summary>
        /// <param name="selectElement">The select element to process.</param>
        /// <param name="context">The current processing context.</param>
        /// <returns>A list of OutputColumn objects produced by this select element.</returns>
        List<OutputColumn> ProcessElement(TSelectElement selectElement, IProcessingContext context);

        // Again, consider if the base Process method is needed or if ProcessElement is sufficient.
        // For now, assume ProcessElement is the primary method.
    }
}
