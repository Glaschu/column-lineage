SELECT 
    p.ProductID,
    p.ProductName,
    p.Price,
    c.CategoryName
FROM Products p
INNER JOIN Categories c ON p.CategoryID = c.CategoryID
WHERE p.Price > 10.00
