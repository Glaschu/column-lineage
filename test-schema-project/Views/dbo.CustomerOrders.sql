CREATE VIEW [dbo].[CustomerOrders]
AS
SELECT 
    c.CustomerID,
    c.CustomerName,
    c.Email,
    o.OrderID,
    o.OrderDate,
    o.TotalAmount,
    o.Status
FROM [dbo].[Customers] c
INNER JOIN [dbo].[Orders] o ON c.CustomerID = o.CustomerID
WHERE c.IsActive = 1
