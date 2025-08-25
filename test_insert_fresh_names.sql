CREATE PROCEDURE test_insert_fresh_names AS
BEGIN
    DECLARE @TestTable TABLE (
        ProdID int,
        PriceCategory varchar(20)
    );
    
    INSERT INTO @TestTable (ProdID, PriceCategory)
    SELECT 
        p.ProductID,
        CASE 
            WHEN p.ListPrice > 1000 THEN 'Expensive'
            WHEN p.ListPrice > 100 THEN 'Moderate'
            ELSE 'Cheap'
        END
    FROM Production.Product p;
    
    SELECT 
        t.ProdID,
        t.PriceCategory
    FROM @TestTable t;
END
