using Microsoft.SqlServer.TransactSql.ScriptDom;
using ColumnLineageCore.Helpers; // For SourceInfo
using System.Collections.Generic;

namespace ColumnLineageCore.Interfaces.Processors
{
    /// <summary>
    /// Defines a contract for processors that handle specific TableReference types (e.g., NamedTableReference, QueryDerivedTable, JoinTableReference).
    /// These processors typically update the context's source map.
    /// </summary>
    public interface ITableReferenceProcessor<in TTableReference> : ISqlFragmentProcessor<TTableReference>
        where TTableReference : TableReference
    {
        // The base Process method inherited from ISqlFragmentProcessor is likely sufficient here.
        // Implementations will interact with context.CurrentSourceMap and potentially context.Graph.
        // void Process(TTableReference fragment, IProcessingContext context);
    }
}
