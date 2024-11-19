CREATE LOGIN AdminUser 
WITH PASSWORD = 'Password2024!',
DEFAULT_DATABASE = [master],
CHECK_EXPIRATION = on,
CHECK_POLICY = on;

ALTER SERVER ROLE sysadmin
ADD MEMBER AdminUser;

USE [master];
CREATE USER AdminUser FOR LOGIN AdminUser;

GRANT CONTROL SERVER TO AdminUser;

SELECT name, type_desc, is_disabled
FROM sys.server_principals
WHERE name = 'AdminUser';
