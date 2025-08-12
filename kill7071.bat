for /f "tokens=5" %%a in ('netstat -aon ^| findstr "7071"') do taskkill /f /pid %%a

exit 0