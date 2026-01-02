$env:HOST_IP = [System.Net.NetworkInformation.NetworkInterface]::GetAllNetworkInterfaces() |
    Where-Object { $_.OperationalStatus -eq 'Up' -and $_.NetworkInterfaceType -ne 'Loopback' } |
    ForEach-Object { $_.GetIPProperties().UnicastAddresses } |
    Where-Object { $_.Address.AddressFamily -eq 'InterNetwork' -and $_.Address.IPAddressToString -notlike '169.254.*' } |
    Select-Object -First 1 -ExpandProperty Address |
    Select-Object -ExpandProperty IPAddressToString

Write-Host "Using machine IP: $env:HOST_IP"
docker compose up --detach
