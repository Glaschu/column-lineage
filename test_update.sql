CREATE PROCEDURE TestUpdateProcedure
AS
BEGIN
    UPDATE Employee 
    SET JobTitle = p.Title,
        ModifiedDate = GETDATE()
    FROM Employee e
    INNER JOIN Person p ON e.BusinessEntityID = p.BusinessEntityID
    WHERE e.OrganizationLevel > 2;
END
