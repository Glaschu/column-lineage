CREATE PROCEDURE test_case_simple AS
BEGIN
    SELECT 
        p.ProductID,
        CASE 
            WHEN p.ListPrice > 100 THEN 'Expensive'
            ELSE 'Cheap'
        END as PriceLevel
    FROM Production.Product p;
END
