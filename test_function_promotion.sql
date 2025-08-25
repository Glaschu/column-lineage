CREATE PROCEDURE test_function_promotion AS
BEGIN
    SELECT 
        p.ProductID,
        UPPER(p.Name) as ProductNameUpper
    FROM Production.Product p;
END
