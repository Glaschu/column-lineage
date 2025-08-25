-- Test Window Functions and Advanced SQL Features
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    sod.OrderQty,
    sod.UnitPrice,
    sod.LineTotal,
    soh.OrderDate,
    c.CustomerID,
    -- Window functions for ranking
    ROW_NUMBER() OVER (PARTITION BY p.ProductID ORDER BY sod.LineTotal DESC) AS ProductSalesRank,
    RANK() OVER (ORDER BY sod.LineTotal DESC) AS OverallSalesRank,
    DENSE_RANK() OVER (PARTITION BY c.CustomerID ORDER BY soh.OrderDate) AS CustomerOrderRank,
    -- Aggregate window functions
    SUM(sod.LineTotal) OVER (PARTITION BY p.ProductID) AS ProductTotalSales,
    AVG(sod.LineTotal) OVER (PARTITION BY p.ProductID) AS ProductAvgSales,
    COUNT(*) OVER (PARTITION BY c.CustomerID) AS CustomerOrderCount,
    -- Offset functions
    LAG(sod.LineTotal, 1, 0) OVER (PARTITION BY p.ProductID ORDER BY soh.OrderDate) AS PreviousSale,
    LEAD(sod.LineTotal, 1, 0) OVER (PARTITION BY p.ProductID ORDER BY soh.OrderDate) AS NextSale,
    FIRST_VALUE(sod.LineTotal) OVER (PARTITION BY p.ProductID ORDER BY soh.OrderDate ROWS UNBOUNDED PRECEDING) AS FirstSale,
    LAST_VALUE(sod.LineTotal) OVER (PARTITION BY p.ProductID ORDER BY soh.OrderDate ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS LastSale,
    -- Statistical functions
    PERCENT_RANK() OVER (ORDER BY sod.LineTotal) AS SalesPercentile,
    CUME_DIST() OVER (ORDER BY sod.LineTotal) AS CumulativeDistribution,
    NTILE(4) OVER (ORDER BY sod.LineTotal) AS SalesQuartile
FROM Production.Product p
INNER JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
INNER JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
INNER JOIN Sales.Customer c ON soh.CustomerID = c.CustomerID
WHERE soh.OrderDate >= '2022-01-01';

-- Test APPLY operations
SELECT 
    c.CustomerID,
    p.FirstName,
    p.LastName,
    ot.OrderCount,
    ot.TotalSpent,
    ot.LastOrderDate,
    TOP3.ProductName,
    TOP3.ProductSales,
    TOP3.ProductRank
FROM Sales.Customer c
INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
CROSS APPLY (
    SELECT 
        COUNT(*) AS OrderCount,
        SUM(TotalDue) AS TotalSpent,
        MAX(OrderDate) AS LastOrderDate
    FROM Sales.SalesOrderHeader soh
    WHERE soh.CustomerID = c.CustomerID
) AS ot
OUTER APPLY (
    SELECT TOP 3
        prod.Name AS ProductName,
        SUM(sod.LineTotal) AS ProductSales,
        ROW_NUMBER() OVER (ORDER BY SUM(sod.LineTotal) DESC) AS ProductRank
    FROM Sales.SalesOrderHeader soh2
    INNER JOIN Sales.SalesOrderDetail sod ON soh2.SalesOrderID = sod.SalesOrderID
    INNER JOIN Production.Product prod ON sod.ProductID = prod.ProductID
    WHERE soh2.CustomerID = c.CustomerID
    GROUP BY prod.ProductID, prod.Name
    ORDER BY SUM(sod.LineTotal) DESC
) AS TOP3
WHERE ot.OrderCount > 0;

-- Test CASE expressions and conditional logic
SELECT 
    p.ProductID,
    p.Name AS ProductName,
    p.ListPrice,
    p.StandardCost,
    -- Complex CASE expressions
    CASE 
        WHEN p.ListPrice > 1000 THEN 'Premium'
        WHEN p.ListPrice > 500 THEN 'Mid-Range'
        WHEN p.ListPrice > 100 THEN 'Budget'
        ELSE 'Economy'
    END AS PriceCategory,
    CASE 
        WHEN p.StandardCost > 0 
        THEN ROUND(((p.ListPrice - p.StandardCost) / p.StandardCost) * 100, 2)
        ELSE 0
    END AS ProfitMarginPercent,
    -- Nested CASE
    CASE 
        WHEN p.SellEndDate IS NULL THEN 
            CASE 
                WHEN p.SellStartDate IS NULL THEN 'Never Sold'
                ELSE 'Currently Selling'
            END
        ELSE 'Discontinued'
    END AS SalesStatus,
    -- CASE in aggregate
    SUM(CASE WHEN sod.OrderQty > 10 THEN 1 ELSE 0 END) AS LargeOrderCount,
    AVG(CASE WHEN sod.UnitPriceDiscount > 0 THEN sod.UnitPrice ELSE NULL END) AS AvgDiscountedPrice
FROM Production.Product p
LEFT JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
GROUP BY p.ProductID, p.Name, p.ListPrice, p.StandardCost, p.SellStartDate, p.SellEndDate;
