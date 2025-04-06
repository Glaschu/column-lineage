using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using ColumnLineageCore.Interfaces; // Added

namespace ColumnLineageCore
{
    /// <summary>
    /// Responsible for parsing SQL text into a TSqlFragment (Abstract Syntax Tree).
    /// </summary>
    public class AstProvider : IAstProvider // Implement interface
    {
        private readonly bool _initialQuotedIdentifiers;
        private readonly SqlEngineType _engineType;

        // Consider making parser options configurable
        public AstProvider(bool initialQuotedIdentifiers = true, SqlEngineType engineType = SqlEngineType.All)
        {
            _initialQuotedIdentifiers = initialQuotedIdentifiers;
            _engineType = engineType;
        }

        /// <summary>
        /// Parses the given SQL script text into a TSqlFragment.
        /// </summary>
        /// <param name="sqlScript">The SQL script text.</param>
        /// <param name="errors">A list of parse errors encountered.</param>
        /// <returns>The root TSqlFragment of the parsed script, or null if parsing fails catastrophically (though parser usually returns a fragment even with errors).</returns>
        // Method signature now matches interface (already public)
        public TSqlFragment? Parse(string sqlScript, out IList<ParseError> errors)
        {
            // Use the configured parser version (TSql160Parser for SQL Server 2022 compatibility)
            var parser = new TSql160Parser(_initialQuotedIdentifiers, _engineType);
            using var reader = new StringReader(sqlScript);
            TSqlFragment? fragment = parser.Parse(reader, out errors);

            if (errors.Any())
            {
                // Log errors or handle them as needed - the caller might decide based on the errors list
                System.Diagnostics.Debug.WriteLine($"[AstProvider] Parse errors encountered: {errors.Count}");
                foreach (var error in errors)
                {
                     System.Diagnostics.Debug.WriteLine($" - {error.Message} (Line: {error.Line}, Col: {error.Column})");
                }
            }

            return fragment;
        }
    }
}
