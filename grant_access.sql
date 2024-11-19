-- Grant server access
USE master;
GO



GRANT CONNECT SQL TO [AdminUser];

SELECT SERVERPROPERTY('IsIntegratedSecurityOnly');

EXEC xp_readerrorlog;


