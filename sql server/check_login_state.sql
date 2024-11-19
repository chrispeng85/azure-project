-- Check if login is disabled or locked
SELECT name, is_disabled, is_policy_checked, is_expiration_checked
FROM sys.sql_logins 
WHERE name = 'AdminUser';

-- Enable if disabled
ALTER LOGIN [AdminUser] ENABLE;