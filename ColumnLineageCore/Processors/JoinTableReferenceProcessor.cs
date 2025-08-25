using ColumnLineageCore.Interfaces;
using ColumnLineageCore.Interfaces.Processors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Processes JoinTableReference fragments (INNER JOIN, LEFT JOIN, etc.).
    /// Delegates processing of the joined table references to the factory.
    /// </summary>
    public class JoinTableReferenceProcessor : ITableReferenceProcessor<JoinTableReference>
    {
        public void Process(JoinTableReference joinRef, IProcessingContext context)
        {
            if (joinRef == null) throw new ArgumentNullException(nameof(joinRef));
            if (context == null) throw new ArgumentNullException(nameof(context));

            // Log the specific join type if available (e.g., on QualifiedJoin)
            string joinTypeDescription = joinRef is QualifiedJoin qj ? qj.QualifiedJoinType.ToString() : joinRef.GetType().Name;
            System.Diagnostics.Debug.WriteLine($"[Processor] Processing Join: {joinTypeDescription}");

            // Recursively process the first table reference
            ProcessChildTableReference(joinRef.FirstTableReference, context);

            // Recursively process the second table reference
            ProcessChildTableReference(joinRef.SecondTableReference, context);

            // TODO: Future enhancement - Process the JOIN condition (joinRef.SearchCondition)
            // This could potentially establish lineage links based on join keys,
            // although it adds significant complexity. For now, we focus on FROM sources.
        }

        /// <summary>
        /// Helper method to process a child table reference using the factory.
        /// </summary>
        private void ProcessChildTableReference(TableReference tableReference, IProcessingContext context)
        {
            // Use dynamic dispatch via the factory based on the actual type of the tableReference
            // (e.g., NamedTableReference, QueryDerivedTable, another JoinTableReference)
            try
            {
                // We need a way to call the factory with the specific type of tableReference.
                // Using reflection or a switch statement on type can be cumbersome.
                // A better approach might be to have the factory handle dynamic dispatch
                // or use a visitor pattern internally within the processor if needed,
                // but for now, let's assume the factory can get the right processor.

                // This dynamic approach is tricky without direct type knowledge.
                // Let's try a more explicit check for known types we handle.
                if (tableReference is NamedTableReference namedRef)
                {
                    var processor = context.ProcessorFactory.GetProcessor(namedRef); // Get specific processor
                    processor.Process(namedRef, context);
                }
                else if (tableReference is QueryDerivedTable derivedRef)
                {
                     // Assuming QueryDerivedTableProcessor exists and is registered
                     var processor = context.ProcessorFactory.GetProcessor(derivedRef);
                     processor.Process(derivedRef, context);
                }
                else if (tableReference is JoinTableReference nestedJoinRef)
                {
                     // Recursive call for nested joins
                     var processor = context.ProcessorFactory.GetProcessor(nestedJoinRef);
                     processor.Process(nestedJoinRef, context);
                }
                else if (tableReference is PivotedTableReference pivotRef)
                {
                     var processor = context.ProcessorFactory.GetProcessor(pivotRef);
                     processor.Process(pivotRef, context);
                }
                else if (tableReference is UnpivotedTableReference unpivotRef)
                {
                     var processor = context.ProcessorFactory.GetProcessor(unpivotRef);
                     processor.Process(unpivotRef, context);
                }
                else if (tableReference is VariableTableReference varRef)
                {
                     var processor = context.ProcessorFactory.GetProcessor(varRef);
                     processor.Process(varRef, context);
                }
                // Add other TableReference types here
                else
                {
                     System.Diagnostics.Debug.WriteLine($"[Processor] Warning: Unsupported child table reference type in JOIN: {tableReference.GetType().Name}");
                }

            }
            catch (Exception ex)
            {
                 System.Diagnostics.Debug.WriteLine($"[Processor] Error processing child table reference in JOIN: {ex.Message} - Type: {tableReference.GetType().Name}");
                 // Decide if we should re-throw or just log and continue
            }
        }
    }
}
