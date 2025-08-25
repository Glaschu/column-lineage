CREATE FUNCTION [dbo].[GetCustomerName]
(
    @CustomerID INT
)
RETURNS NVARCHAR(100)
AS
BEGIN
    DECLARE @CustomerName NVARCHAR(100);
    
    SELECT @CustomerName = CustomerName
    FROM [dbo].[Customers]
    WHERE CustomerID = @CustomerID;
    
    RETURN ISNULL(@CustomerName, 'Unknown Customer');
END
