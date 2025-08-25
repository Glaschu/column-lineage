CREATE PROCEDURE complex_table_variable_test AS
BEGIN
    -- First table variable
    DECLARE @ProductInfo TABLE (
        ProductID int,
        ProductName varchar(50),
        BasePrice decimal(10,2),
        CategoryName varchar(50)
    );

    -- Second table variable
    DECLARE @SalesData TABLE (
        ProductID int,
        TotalSales decimal(10,2),
        SalesCount int
    );

    -- Populate first table variable with JOIN
    INSERT INTO @ProductInfo (ProductID, ProductName, BasePrice, CategoryName)
    SELECT 
        p.ProductID, 
        p.Name,
        p.ListPrice,
        pc.Name
    FROM Production.Product p
    INNER JOIN Production.ProductCategory pc ON p.ProductSubcategoryID = pc.ProductCategoryID;

    -- Populate second table variable with aggregation
    INSERT INTO @SalesData (ProductID, TotalSales, SalesCount)
    SELECT 
        sod.ProductID,
        SUM(sod.LineTotal),
        COUNT(*)
    FROM Sales.SalesOrderDetail sod
    GROUP BY sod.ProductID;

    -- Final query joining both table variables with original tables
    SELECT 
        pi.ProductID,
        pi.ProductName,
        CASE 
            WHEN pi.BasePrice > 1000 THEN 'Premium'
            ELSE 'Standard'
        END as PriceLevel,
        CAST(sd.TotalSales as varchar(20)) as TotalSalesText,
        sd.SalesCount,
        p.Color
    FROM @ProductInfo pi
    INNER JOIN @SalesData sd ON pi.ProductID = sd.ProductID
    INNER JOIN Production.Product p ON pi.ProductID = p.ProductID
    WHERE sd.TotalSales > 1000;
END
