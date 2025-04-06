using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;

namespace ColumnLineageCore.Interfaces
{
    /// <summary>
    /// Defines the contract for a factory that provides appropriate processors
    /// for different TSqlFragment types during lineage analysis.
    /// </summary>
    public interface IProcessorFactory
    {
        /// <summary>
        /// Gets the processor suitable for handling the specified TSqlFragment type.
        /// </summary>
        /// <typeparam name="TFragment">The type of the TSqlFragment.</typeparam>
        /// <param name="fragment">The specific fragment instance (optional, might be needed for context by some factories).</param>
        /// <returns>An instance of the appropriate processor.</returns>
        /// <exception cref="NotSupportedException">Thrown if no processor is registered for the given fragment type.</exception>
        ISqlFragmentProcessor<TFragment> GetProcessor<TFragment>(TFragment? fragment = null) where TFragment : TSqlFragment;

        // Consider adding registration methods if the factory implementation will allow dynamic registration,
        // otherwise registration might happen via constructor injection or a dedicated builder.
        // void RegisterProcessor<TFragment, TProcessor>() where TFragment : TSqlFragment where TProcessor : ISqlFragmentProcessor<TFragment>;
    }

    // Base interface for all fragment processors (can be generic)
    // This allows the factory to return a common type.
    // Processors will likely implement more specific interfaces derived from this.
    public interface ISqlFragmentProcessor<in TFragment> where TFragment : TSqlFragment
    {
        /// <summary>
        /// Processes the given SQL fragment within the provided context.
        /// </summary>
        /// <param name="fragment">The SQL fragment to process.</param>
        /// <param name="context">The current processing context.</param>
        void Process(TFragment fragment, IProcessingContext context);
    }
}
