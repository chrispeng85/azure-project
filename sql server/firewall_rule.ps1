New-NetFirewallRule -DisplayName "SQL Server Express" `
    -Direction Inbound `
    -Protocol TCP `
    -LocalPort 1433 `
    -Action Allow 
    