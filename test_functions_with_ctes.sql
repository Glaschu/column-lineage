-- Test Table-Valued Functions with Multiple CTEs
CREATE FUNCTION dbo.GetCustomerOrderSummary
(
    @StartDate DATE,
    @EndDate DATE,
    @TerritoryID INT = NULL
)
RETURNS TABLE
AS
RETURN
(
    WITH CustomerBase AS (
        SELECT 
            c.CustomerID,
            p.FirstName,
            p.LastName,
            p.FirstName + ' ' + p.LastName AS FullName,
            c.TerritoryID,
            st.Name AS TerritoryName
        FROM Sales.Customer c
        INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
        LEFT JOIN Sales.SalesTerritory st ON c.TerritoryID = st.TerritoryID
        WHERE (@TerritoryID IS NULL OR c.TerritoryID = @TerritoryID)
    ),
    OrderTotals AS (
        SELECT 
            soh.CustomerID,
            COUNT(*) AS OrderCount,
            SUM(soh.SubTotal) AS TotalSubTotal,
            SUM(soh.TaxAmt) AS TotalTax,
            SUM(soh.TotalDue) AS TotalDue,
            AVG(soh.SubTotal) AS AverageOrderValue,
            MIN(soh.OrderDate) AS FirstOrderDate,
            MAX(soh.OrderDate) AS LastOrderDate
        FROM Sales.SalesOrderHeader soh
        WHERE soh.OrderDate BETWEEN @StartDate AND @EndDate
        GROUP BY soh.CustomerID
    ),
    ProductSummary AS (
        SELECT 
            soh.CustomerID,
            COUNT(DISTINCT sod.ProductID) AS UniqueProducts,
            SUM(sod.OrderQty) AS TotalQuantity,
            COUNT(DISTINCT pc.ProductCategoryID) AS UniqueCategories
        FROM Sales.SalesOrderHeader soh
        INNER JOIN Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
        INNER JOIN Production.Product p ON sod.ProductID = p.ProductID
        LEFT JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
        LEFT JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
        WHERE soh.OrderDate BETWEEN @StartDate AND @EndDate
        GROUP BY soh.CustomerID
    ),
    FinalResults AS (
        SELECT 
            cb.CustomerID,
            cb.FirstName,
            cb.LastName,
            cb.FullName,
            cb.TerritoryName,
            ISNULL(ot.OrderCount, 0) AS OrderCount,
            ISNULL(ot.TotalDue, 0) AS TotalDue,
            ISNULL(ot.AverageOrderValue, 0) AS AverageOrderValue,
            ISNULL(ps.UniqueProducts, 0) AS UniqueProducts,
            ISNULL(ps.TotalQuantity, 0) AS TotalQuantity,
            ISNULL(ps.UniqueCategories, 0) AS UniqueCategories,
            ot.FirstOrderDate,
            ot.LastOrderDate,
            CASE 
                WHEN ot.OrderCount >= 10 THEN 'High Volume'
                WHEN ot.OrderCount >= 5 THEN 'Medium Volume'
                WHEN ot.OrderCount >= 1 THEN 'Low Volume'
                ELSE 'No Orders'
            END AS CustomerSegment
        FROM CustomerBase cb
        LEFT JOIN OrderTotals ot ON cb.CustomerID = ot.CustomerID
        LEFT JOIN ProductSummary ps ON cb.CustomerID = ps.CustomerID
    )
    SELECT 
        CustomerID,
        FirstName,
        LastName,
        FullName,
        TerritoryName,
        OrderCount,
        TotalDue,
        AverageOrderValue,
        UniqueProducts,
        TotalQuantity,
        UniqueCategories,
        FirstOrderDate,
        LastOrderDate,
        CustomerSegment
    FROM FinalResults
);

-- Test function with Window Functions and Multiple CTEs
CREATE FUNCTION dbo.GetProductSalesRanking
(
    @Year INT
)
RETURNS TABLE
AS
RETURN
(
    WITH ProductSales AS (
        SELECT 
            p.ProductID,
            p.Name AS ProductName,
            pc.Name AS CategoryName,
            psc.Name AS SubcategoryName,
            SUM(sod.OrderQty) AS TotalQuantity,
            SUM(sod.LineTotal) AS TotalSales,
            COUNT(DISTINCT soh.CustomerID) AS UniqueCustomers,
            COUNT(*) AS OrderLines
        FROM Production.Product p
        INNER JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
        INNER JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
        LEFT JOIN Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
        LEFT JOIN Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
        WHERE YEAR(soh.OrderDate) = @Year
        GROUP BY p.ProductID, p.Name, pc.Name, psc.Name
    ),
    RankedProducts AS (
        SELECT 
            ProductID,
            ProductName,
            CategoryName,
            SubcategoryName,
            TotalQuantity,
            TotalSales,
            UniqueCustomers,
            OrderLines,
            ROW_NUMBER() OVER (ORDER BY TotalSales DESC) AS OverallRank,
            ROW_NUMBER() OVER (PARTITION BY CategoryName ORDER BY TotalSales DESC) AS CategoryRank,
            RANK() OVER (ORDER BY TotalSales DESC) AS SalesRank,
            DENSE_RANK() OVER (ORDER BY UniqueCustomers DESC) AS CustomerRank,
            PERCENT_RANK() OVER (ORDER BY TotalSales) AS SalesPercentile,
            NTILE(10) OVER (ORDER BY TotalSales DESC) AS SalesDecile
        FROM ProductSales
        WHERE TotalSales > 0
    ),
    FinalRanking AS (
        SELECT 
            *,
            LAG(TotalSales) OVER (ORDER BY OverallRank) AS PreviousProductSales,
            LEAD(TotalSales) OVER (ORDER BY OverallRank) AS NextProductSales,
            SUM(TotalSales) OVER (PARTITION BY CategoryName) AS CategoryTotalSales,
            AVG(TotalSales) OVER (PARTITION BY CategoryName) AS CategoryAvgSales,
            COUNT(*) OVER (PARTITION BY CategoryName) AS CategoryProductCount
        FROM RankedProducts
    )
    SELECT 
        ProductID,
        ProductName,
        CategoryName,
        SubcategoryName,
        TotalQuantity,
        TotalSales,
        UniqueCustomers,
        OrderLines,
        OverallRank,
        CategoryRank,
        SalesRank,
        CustomerRank,
        SalesPercentile,
        SalesDecile,
        PreviousProductSales,
        NextProductSales,
        CategoryTotalSales,
        CategoryAvgSales,
        CategoryProductCount
    FROM FinalRanking
);
