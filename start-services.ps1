$env:HOST_IP = [System.Net.Dns]::GetHostAddresses([System.Net.Dns]::GetHostName()) |
Where-Object {
    $_.AddressFamily -eq [System.Net.Sockets.AddressFamily]::InterNetwork -and
    $_.IPAddressToString -notin '127.0.0.1'
} |
Select-Object -First 1 -ExpandProperty IPAddressToString

Write-Host "Using machine IP: $env:HOST_IP"
docker compose up --detach
