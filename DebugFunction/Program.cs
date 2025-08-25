using System;
using System.IO;
using Microsoft.SqlServer.TransactSql.ScriptDom;

class DebugFunction
{
    static void Main()
    {
        string sql = @"SELECT CategoryName, [1] AS January, [2] AS February
FROM CategoryTotals
PIVOT (
    SUM(MonthlyAmount)
    FOR SalesMonth IN ([1], [2])
) AS PivotResult";
        
        var parser = new TSql160Parser(true);
        IList<ParseError> errors;
        var fragment = parser.Parse(new StringReader(sql), out errors);
        
        if (fragment != null && errors.Count == 0)
        {
            var visitor = new DebugVisitor();
            fragment.Accept(visitor);
        }
        else
        {
            Console.WriteLine($"Parse errors: {errors.Count}");
            foreach (var error in errors)
                Console.WriteLine($"  {error.Message}");
        }
    }
}

class DebugVisitor : TSqlFragmentVisitor
{
    public override void Visit(TSqlFragment node)
    {
        Console.WriteLine($"Visiting: {node.GetType().Name}");
        if (node.GetType().Name.ToLower().Contains("pivot"))
        {
            Console.WriteLine($"  PIVOT element found: {node.GetType().Name}");
            var properties = node.GetType().GetProperties();
            foreach (var prop in properties)
            {
                try
                {
                    var value = prop.GetValue(node);
                    Console.WriteLine($"    {prop.Name}: {value?.GetType().Name ?? "null"}");
                }
                catch
                {
                    Console.WriteLine($"    {prop.Name}: <error>");
                }
            }
        }
        base.Visit(node);
    }
}
