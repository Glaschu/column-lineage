using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace ColumnLineageCore.Interfaces
{
    /// <summary>
    /// Defines the contract for parsing SQL text into an AST.
    /// </summary>
    public interface IAstProvider
    {
        /// <summary>
        /// Parses the given SQL script text into a TSqlFragment.
        /// </summary>
        /// <param name="sqlScript">The SQL script text.</param>
        /// <param name="errors">A list of parse errors encountered.</param>
        /// <returns>The root TSqlFragment of the parsed script, or null if parsing fails catastrophically.</returns>
        TSqlFragment? Parse(string sqlScript, out IList<ParseError> errors);
    }
}
