CREATE FUNCTION dbo.TestFunction()
RETURNS TABLE
AS
RETURN
(
    SELECT 
        CustomerID,
        FirstName
    FROM Sales.Customer c
    INNER JOIN Person.Person p ON c.PersonID = p.BusinessEntityID
)
