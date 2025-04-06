using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using ColumnLineageCore.Model;

namespace ColumnLineageCore.Helpers // Corrected namespace
{
    public class CteInfo // Changed internal to public
    {
        public string Name { get; }
        public QueryExpression Definition { get; }
        /// <summary>Maps the output column name of the CTE to its direct source node within the CTE definition.</summary>
        public Dictionary<string, ColumnNode> OutputColumnSources { get; } = new Dictionary<string, ColumnNode>(StringComparer.OrdinalIgnoreCase);
        public bool IsProcessed { get; set; } = false; // Flag to avoid reprocessing/cycles

        public CteInfo(string name, QueryExpression definition)
        {
             Name = name;
             Definition = definition;
        }
    }
}
