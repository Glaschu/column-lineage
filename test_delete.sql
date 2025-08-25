DELETE p1
FROM Production.Product p1
INNER JOIN Production.ProductCategory pc ON p1.ProductSubcategoryID = pc.ProductCategoryID
WHERE pc.Name = 'Obsolete' AND p1.ListPrice < 10;
