CREATE PROCEDURE [dbo].[GetCustomerOrders]
    @CustomerID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        CustomerID,
        CustomerName,
        Email,
        OrderID,
        OrderDate,
        TotalAmount,
        Status
    FROM [dbo].[CustomerOrders]
    WHERE CustomerID = @CustomerID
    ORDER BY OrderDate DESC;
END
