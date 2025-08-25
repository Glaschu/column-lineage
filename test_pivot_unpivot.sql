-- Test Cases for PIVOT and UNPIVOT Operations
-- Basic PIVOT Example
SELECT 
    ProductName,
    [2019] AS Sales2019,
    [2020] AS Sales2020,
    [2021] AS Sales2021,
    [2022] AS Sales2022
FROM (
    SELECT 
        p.Name AS ProductName,
        YEAR(soh.OrderDate) AS OrderYear,
        sod.LineTotal
    FROM Production.Product p
    INNER JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
    INNER JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
    WHERE YEAR(soh.OrderDate) BETWEEN 2019 AND 2022
) AS SourceData
PIVOT (
    SUM(LineTotal)
    FOR OrderYear IN ([2019], [2020], [2021], [2022])
) AS PivotTable;

-- Complex PIVOT with CTEs
WITH MonthlySales AS (
    SELECT 
        p.ProductID,
        p.Name AS ProductName,
        pc.Name AS CategoryName,
        MONTH(soh.OrderDate) AS SalesMonth,
        YEAR(soh.OrderDate) AS SalesYear,
        SUM(sod.OrderQty) AS TotalQuantity,
        SUM(sod.LineTotal) AS TotalAmount
    FROM Production.Product p
    INNER JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
    INNER JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
    INNER JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
    INNER JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
    WHERE soh.OrderDate >= '2022-01-01'
    GROUP BY p.ProductID, p.Name, pc.Name, MONTH(soh.OrderDate), YEAR(soh.OrderDate)
),
CategoryTotals AS (
    SELECT 
        CategoryName,
        SalesMonth,
        SUM(TotalQuantity) AS MonthlyQuantity,
        SUM(TotalAmount) AS MonthlyAmount
    FROM MonthlySales
    GROUP BY CategoryName, SalesMonth
)
SELECT 
    CategoryName,
    ISNULL([1], 0) AS January,
    ISNULL([2], 0) AS February,
    ISNULL([3], 0) AS March,
    ISNULL([4], 0) AS April,
    ISNULL([5], 0) AS May,
    ISNULL([6], 0) AS June,
    ISNULL([7], 0) AS July,
    ISNULL([8], 0) AS August,
    ISNULL([9], 0) AS September,
    ISNULL([10], 0) AS October,
    ISNULL([11], 0) AS November,
    ISNULL([12], 0) AS December
FROM CategoryTotals
PIVOT (
    SUM(MonthlyAmount)
    FOR SalesMonth IN ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11], [12])
) AS PivotResult;

-- UNPIVOT Example
WITH SalesData AS (
    SELECT 
        'Product A' AS ProductName,
        1000 AS Q1Sales,
        1200 AS Q2Sales,
        1100 AS Q3Sales,
        1300 AS Q4Sales
    UNION ALL
    SELECT 
        'Product B' AS ProductName,
        800 AS Q1Sales,
        900 AS Q2Sales,
        950 AS Q3Sales,
        1000 AS Q4Sales
)
SELECT 
    ProductName,
    Quarter,
    SalesAmount
FROM SalesData
UNPIVOT (
    SalesAmount FOR Quarter IN (Q1Sales, Q2Sales, Q3Sales, Q4Sales)
) AS UnpivotedData;
