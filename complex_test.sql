-- Test query with various data types
SELECT 
    o.OrderID,
    o.OrderDate,
    c.CustomerName,
    p.ProductName,
    od.Quantity,
    od.UnitPrice,
    (od.Quantity * od.UnitPrice) AS TotalAmount,
    CASE 
        WHEN o.OrderDate >= '2023-01-01' THEN 'Recent'
        ELSE 'Historical' 
    END AS OrderPeriod
FROM Orders o
INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
INNER JOIN Products p ON od.ProductID = p.ProductID
INNER JOIN Customers c ON o.CustomerID = c.CustomerID
WHERE o.OrderDate >= '2022-01-01';
