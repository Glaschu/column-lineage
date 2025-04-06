using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Collections.Generic;

namespace ColumnLineageCore.Processors
{
    /// <summary>
    /// Simple visitor to collect ColumnReferenceExpressions within any expression tree.
    /// Overrides Visit methods for common container expressions to ensure recursive traversal.
    /// </summary>
    internal class ColumnReferenceFinder : TSqlFragmentVisitor
    {
        public List<ColumnReferenceExpression> ColumnReferences { get; } = new List<ColumnReferenceExpression>();

        public override void ExplicitVisit(ColumnReferenceExpression node)
        {
            ColumnReferences.Add(node);
            // Stop traversal here for ColumnReferenceExpression itself
        }

        // Override Visit methods for container expressions to ensure traversal continues
        public override void Visit(FunctionCall node)
        {
            base.Visit(node); // Traverses Parameters and OverClause children
        }

        public override void Visit(BinaryExpression node)
        {
            base.Visit(node); // Traverses FirstExpression and SecondExpression
        }

        public override void Visit(CaseExpression node)
        {
             base.Visit(node); // Traverses InputExpression, WhenClauses, ElseExpression
        }

        public override void Visit(CastCall node)
        {
             base.Visit(node); // Traverses Parameter
        }

        public override void Visit(ConvertCall node)
        {
             base.Visit(node); // Traverses Parameter
        }

        public override void Visit(CoalesceExpression node)
        {
             base.Visit(node); // Traverses Expressions
        }

         public override void Visit(NullIfExpression node)
        {
             base.Visit(node); // Traverses First/Second Expression
        }

         public override void Visit(ParenthesisExpression node)
        {
             base.Visit(node); // Traverses Expression
        }

         public override void Visit(UnaryExpression node)
        {
             base.Visit(node); // Traverses Expression
        }

         // Add more overrides as needed for other expression types...
    }
}
