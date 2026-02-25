def gen_host_full_scan_script(usr: str, pwd: str) -> str:
    return f"""
    $OutputEncoding = [System.Text.Encoding]::UTF8
    [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
    $ErrorActionPreference = 'SilentlyContinue'

    Import-Module Hyper-V -ErrorAction SilentlyContinue

    $allVms = Get-VM
    if (-not $allVms) {{ Write-Output ""; exit 0 }}

    $secPwd = '{pwd}' | ConvertTo-SecureString -asPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential('{usr}', $secPwd)

    # Получаем сети и диски разом для всех машин
    $allNics = Get-VMNetworkAdapter -VM $allVms -ErrorAction SilentlyContinue | Group-Object VMName
    $allDisks = Get-VMHardDiskDrive -VM $allVms -ErrorAction SilentlyContinue | Group-Object VMName

    $vmDict = @{{}}
    foreach ($vm in $allVms) {{
        $macStr = $null
        $vmNics = $allNics | Where-Object Name -eq $vm.Name
        if ($vmNics) {{ $macStr = ($vmNics.Group.MacAddress | Where-Object {{ $_ }}) -join "," }}
        
        $totalDisk = 0
        $vmDisks = $allDisks | Where-Object Name -eq $vm.Name
        if ($vmDisks) {{
            foreach ($disk in $vmDisks.Group) {{
                try {{
                    $p = $disk.Path
                    if ($p -and (Test-Path $p)) {{
                        $vol = Get-VHD -Path $p
                        while ($vol.ParentPath -and (Test-Path $vol.ParentPath)) {{
                            $vol = Get-VHD -Path $vol.ParentPath
                        }}
                        $totalDisk += $vol.Size
                    }}
                }} catch {{ }}
            }}
        }}

        $vmDict[$vm.Name] = @{{
            name = $vm.Name; guid = $vm.Id.Guid; cpu = $vm.ProcessorCount
            ram_mb = [math]::Round($vm.MemoryAssigned / 1MB); storage = $totalDisk
            mac_address = $macStr; power_state = if ($vm.State -eq 2) {{ "running" }} else {{ "off" }}
            domain = $null; os = $null; ip_address = $null; software = @()
        }}
    }}

    $runningVMs = $allVms | Where-Object {{ $_.State -eq 2 }} | Select-Object -ExpandProperty Name

    if ($runningVMs -and $runningVMs.Count -gt 0) {{
        $inGuestResults = Invoke-Command -VMName $runningVMs -Credential $creds -ThrottleLimit 15 -ScriptBlock {{
            $ErrorActionPreference = 'SilentlyContinue'
            
            $sys = Get-CimInstance Win32_ComputerSystem
            $dom = $sys.Domain
            $osData = (Get-CimInstance Win32_OperatingSystem).Caption
            
            $ipObj = Get-NetIPAddress -AddressFamily IPv4 -PrefixOrigin Dhcp,Manual | Where-Object InterfaceAlias -NotMatch 'Loopback' | Select-Object -First 1
            $ipAddr = if ($ipObj) {{ $ipObj.IPAddress }} else {{ $null }}
            
            $regLocs = 'HKLM:\\Software\\Wow6432Node\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\*', 'HKLM:\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\*'   
            $apps = Get-ItemProperty $regLocs | Where-Object {{ $_.DisplayName -and -not $_.SystemComponent }}
            
            
            $cleanApps = New-Object System.Collections.Generic.List[System.Object]
            if ($apps) {{
                $hash = @{{}}
                foreach ($app in $apps) {{
                    $name = ($app.DisplayName).Trim()
                    if (-not [string]::IsNullOrEmpty($name) -and -not $hash.ContainsKey($name)) {{
                        $hash[$name] = $true
                        $cleanApps.Add(@{{ name = $name; version = $app.DisplayVersion }})
                    }}
                }}
            }}
            
            return @{{ Os = $osData; Dom = $dom; Addr = $ipAddr; Apps = $cleanApps }}
        }} -ErrorAction SilentlyContinue

        foreach ($res in $inGuestResults) {{
            $vmName = $res.PSComputerName
            if ($vmName -and $vmDict.ContainsKey($vmName)) {{
                $vmDict[$vmName].os = $res.Os
                $vmDict[$vmName].domain = $res.Dom
                $vmDict[$vmName].ip_address = $res.Addr
                if ($res.Apps) {{ $vmDict[$vmName].software = $res.Apps }}
            }}
        }}
    }}

    $jsonStr = $vmDict.Values | ConvertTo-Json -Depth 4 -Compress
    if ([string]::IsNullOrWhiteSpace($jsonStr)) {{ exit 0 }}

    $jsonBytes = [System.Text.Encoding]::UTF8.GetBytes($jsonStr)
    $ms = New-Object System.IO.MemoryStream
    $cs = New-Object System.IO.Compression.GZipStream($ms, [System.IO.Compression.CompressionMode]::Compress)
    $cs.Write($jsonBytes, 0, $jsonBytes.Length)
    $cs.Close()

    [Console]::WriteLine([Convert]::ToBase64String($ms.ToArray()))
    """