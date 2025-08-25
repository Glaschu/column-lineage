using System;
using System.IO;
using Microsoft.SqlServer.TransactSql.ScriptDom;

class DebugFunction
{
    static void Main()
    {
        string sql = @"CREATE FUNCTION dbo.GetCustomerOrderSummary()
RETURNS TABLE
AS
RETURN
(
    SELECT CustomerID, FirstName FROM Sales.Customer
)";
        
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
    public override void Visit(CreateFunctionStatement node)
    {
        Console.WriteLine("CreateFunctionStatement found!");
        Console.WriteLine($"  ReturnType: {node.ReturnType?.GetType().Name}");
        Console.WriteLine($"  StatementList: {node.StatementList != null}");
        
        // Let's examine what properties are available
        var properties = node.GetType().GetProperties();
        foreach (var prop in properties)
        {
            try
            {
                var value = prop.GetValue(node);
                Console.WriteLine($"  {prop.Name}: {value?.GetType().Name ?? "null"}");
            }
            catch
            {
                Console.WriteLine($"  {prop.Name}: <error>");
            }
        }
        
        base.Visit(node);
    }
    
    public override void Visit(TSqlFragment node)
    {
        Console.WriteLine($"Visiting: {node.GetType().Name}");
        base.Visit(node);
    }
}
