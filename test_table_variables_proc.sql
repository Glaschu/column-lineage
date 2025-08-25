CREATE PROCEDURE test_table_variables_proc AS
BEGIN
    DECLARE @TempProducts TABLE (
        ProductID int,
        ProductName varchar(50),
        Price decimal(10,2)
    );

    INSERT INTO @TempProducts (ProductID, ProductName, Price)
    SELECT ProductID, Name, ListPrice
    FROM Production.Product 
    WHERE ListPrice > 100;

    SELECT 
        tp.ProductID,
        tp.ProductName,
        tp.Price as CurrentPrice,
        p.Color
    FROM @TempProducts tp
    INNER JOIN Production.Product p ON tp.ProductID = p.ProductID;
END
