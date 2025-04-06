using ColumnLineageCore.Interfaces;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;

namespace ColumnLineageCore
{
    /// <summary>
    /// Concrete implementation of IProcessorFactory using a dictionary lookup.
    /// Processors are typically registered during application startup or via dependency injection.
    /// </summary>
    public class ProcessorFactory : IProcessorFactory
    {
        // Dictionary mapping the TSqlFragment Type to a function that creates the processor instance.
        // Using Func allows for lazy instantiation or dependency injection if needed later.
        private readonly Dictionary<Type, Func<object>> _processorRegistry = new Dictionary<Type, Func<object>>();

        // --- Registration Methods (Example - could be done via DI container instead) ---

        /// <summary>
        /// Registers a processor type for a specific TSqlFragment type.
        /// Assumes the processor has a parameterless constructor or dependencies are handled elsewhere.
        /// </summary>
        public void RegisterProcessor<TFragment, TProcessor>()
            where TFragment : TSqlFragment
            where TProcessor : ISqlFragmentProcessor<TFragment>, new() // Constraint for simple instantiation
        {
            _processorRegistry[typeof(TFragment)] = () => new TProcessor();
            System.Diagnostics.Debug.WriteLine($"[ProcessorFactory] Registered {typeof(TProcessor).Name} for {typeof(TFragment).Name}");
        }

        /// <summary>
        /// Registers a processor instance directly (useful for singletons or pre-configured instances).
        /// </summary>
        public void RegisterProcessorInstance<TFragment>(ISqlFragmentProcessor<TFragment> processorInstance)
             where TFragment : TSqlFragment
        {
             if (processorInstance == null) throw new ArgumentNullException(nameof(processorInstance));
            _processorRegistry[typeof(TFragment)] = () => processorInstance;
             System.Diagnostics.Debug.WriteLine($"[ProcessorFactory] Registered instance of {processorInstance.GetType().Name} for {typeof(TFragment).Name}");
        }

         /// <summary>
        /// Registers a factory function for creating a processor (allows for complex instantiation or DI).
        /// </summary>
        public void RegisterProcessorFactory<TFragment>(Func<ISqlFragmentProcessor<TFragment>> factoryFunc)
             where TFragment : TSqlFragment
        {
             if (factoryFunc == null) throw new ArgumentNullException(nameof(factoryFunc));
            _processorRegistry[typeof(TFragment)] = () => factoryFunc()!; // Store the Func<object>
             System.Diagnostics.Debug.WriteLine($"[ProcessorFactory] Registered factory function for {typeof(TFragment).Name}");
        }


        // --- GetProcessor Implementation ---

        /// <summary>
        /// Gets the processor suitable for handling the specified TSqlFragment type.
        /// </summary>
        public ISqlFragmentProcessor<TFragment> GetProcessor<TFragment>(TFragment? fragment = null)
            where TFragment : TSqlFragment
        {
            if (!_processorRegistry.TryGetValue(typeof(TFragment), out var factoryFunc))
            {
                // Optional: Could check base types if direct type not found
                // For now, throw if exact type match isn't registered.
                throw new NotSupportedException($"No processor registered for fragment type: {typeof(TFragment).FullName}");
            }

            // Invoke the factory function to get/create the processor instance
            var processorObject = factoryFunc();

            if (processorObject is ISqlFragmentProcessor<TFragment> specificProcessor)
            {
                return specificProcessor;
            }
            else
            {
                // This should ideally not happen if registration is correct, but good to check.
                throw new InvalidOperationException($"Registered factory for {typeof(TFragment).FullName} returned an incompatible processor type: {processorObject?.GetType().FullName}");
            }
        }
    }
}
