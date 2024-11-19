SELECT name, type_desc, is_disabled
FROM sys.server_principals
WHERE name = 'AdminUser';

-- Check database user mapping
SELECT dp.name as database_user, dp.type_desc, sp.name as login_name
FROM sys.database_principals dp
LEFT JOIN sys.server_principals sp ON dp.sid = sp.sid
WHERE sp.name = 'AdminUser';