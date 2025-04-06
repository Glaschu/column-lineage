using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;
using System.Linq;

namespace ColumnLineageCore.Helpers
{
    /// <summary>
    /// Helper utility to extract information from TSqlScriptDom objects.
    /// </summary>
    public static class ParameterExtractor // Made public
    {
        /// <summary>
        /// Parses a CREATE PROCEDURE statement and extracts its parameters.
        /// </summary>
        public static List<ProcedureParameter>? ExtractProcedureParameters(string procedureDefinitionSql)
        {
            var parser = new TSql160Parser(true); // Or appropriate version
            var fragment = parser.Parse(new System.IO.StringReader(procedureDefinitionSql), out IList<ParseError> errors);

            if (errors.Any() || fragment == null)
            {
                System.Diagnostics.Debug.WriteLine($"[ParameterExtractor] Error parsing procedure definition: {string.Join(", ", errors.Select(e => e.Message))}");
                return null;
            }

            // Find the CreateProcedureStatement
            CreateProcedureStatement? createProcStmt = null;
            var visitor = new ProcedureFinder();
            fragment.Accept(visitor);
            createProcStmt = visitor.ProcedureStatement;


            if (createProcStmt != null)
            {
                return createProcStmt.Parameters?.ToList();
            }

            System.Diagnostics.Debug.WriteLine($"[ParameterExtractor] Could not find CreateProcedureStatement in provided SQL.");
            return null;
        }

        // Simple visitor to find the first CreateProcedureStatement
        private class ProcedureFinder : TSqlFragmentVisitor
        {
            public CreateProcedureStatement? ProcedureStatement { get; private set; }

            public override void Visit(CreateProcedureStatement node)
            {
                if (ProcedureStatement == null) // Take the first one found
                {
                    ProcedureStatement = node;
                }
                // Don't call base.Visit to avoid going into nested procedures if any
            }
        }
    }
}
