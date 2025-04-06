using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace ColumnLineageCore.Interfaces.Processors
{
    /// <summary>
    /// Defines a contract for processors that handle specific TSqlStatement types.
    /// Inherits the base processing logic from ISqlFragmentProcessor.
    /// </summary>
    public interface IStatementProcessor<in TStatement> : ISqlFragmentProcessor<TStatement>
        where TStatement : TSqlStatement
    {
        // Currently inherits Process method.
        // Can add statement-specific methods later if needed.
    }
}
