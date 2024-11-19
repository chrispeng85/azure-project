-- Check if user mapping exists in the database
USE AdventureWorksLT2022;
SELECT * FROM sys.database_principals 
WHERE name = 'AdminUser';

-- Create user mapping if it doesn't exist
/*IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'AdminUser')
BEGIN
    CREATE USER [AdminUser] FOR LOGIN [AdminUser];
    ALTER ROLE db_datareader ADD MEMBER [AdminUser];
    ALTER ROLE db_datawriter ADD MEMBER [AdminUser];
END

*/