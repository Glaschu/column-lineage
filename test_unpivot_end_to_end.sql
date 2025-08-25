CREATE PROCEDURE test_unpivot_end_to_end AS
BEGIN
    -- Create source data with PIVOT
    WITH PivotSource AS (
        SELECT 
            ProductID,
            [1] as Q1Sales,
            [2] as Q2Sales,
            [3] as Q3Sales,
            [4] as Q4Sales
        FROM (
            SELECT 
                p.ProductID,
                DATEPART(quarter, soh.OrderDate) as Quarter,
                SUM(sod.LineTotal) as SalesAmount
            FROM Production.Product p
            INNER JOIN Sales.SalesOrderDetail sod ON p.ProductID = sod.ProductID
            INNER JOIN Sales.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
            GROUP BY p.ProductID, DATEPART(quarter, soh.OrderDate)
        ) as SourceData
        PIVOT (
            SUM(SalesAmount)
            FOR Quarter IN ([1], [2], [3], [4])
        ) as PivotTable
    )
    -- Now UNPIVOT the data back
    SELECT 
        ps.ProductID,
        up.Quarter,
        up.SalesAmount,
        p.Name as ProductName
    FROM PivotSource ps
    UNPIVOT (
        SalesAmount FOR Quarter IN (Q1Sales, Q2Sales, Q3Sales, Q4Sales)
    ) as up
    INNER JOIN Production.Product p ON ps.ProductID = p.ProductID;
END
