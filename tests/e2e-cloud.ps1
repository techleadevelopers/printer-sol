[CmdletBinding()]
param(
  [string]$BaseUrl = 'http://54.196.187.210:8080',
  [string]$AdminPassword = $env:ADMIN_PASSWORD,
  [int]$TimeoutSeconds = 20
)

# Cloud E2E smoke test. It authenticates the console only; it never changes
# bot state, RPC selection, configuration, or sends a transaction.
$ErrorActionPreference = 'Stop'
$baseUri = $BaseUrl.TrimEnd('/')

function Assert-That {
  param([bool]$Condition, [string]$Message)
  if (-not $Condition) { throw "Assertion failed: $Message" }
}

function Invoke-Api {
  param(
    [Parameter(Mandatory)][string]$Path,
    [ValidateSet('GET', 'POST')][string]$Method = 'GET',
    [hashtable]$Headers = @{},
    $Body
  )
  $request = @{ Uri = "$baseUri$Path"; Method = $Method; Headers = $Headers; TimeoutSec = $TimeoutSeconds; UseBasicParsing = $true }
  if ($null -ne $Body) {
    $request.ContentType = 'application/json'
    $request.Body = $Body | ConvertTo-Json -Compress
  }
  $response = Invoke-WebRequest @request
  Assert-That ($response.StatusCode -eq 200) "$Path returned HTTP $($response.StatusCode)"
  $payload = $response.Content | ConvertFrom-Json
  Assert-That ($null -ne $payload.data) "$Path did not return the expected data envelope"
  return $payload
}

function Assert-StartRouteDisabled {
  try {
    Invoke-WebRequest -Uri "$baseUri/api/bot/start" -Method POST -TimeoutSec $TimeoutSeconds -UseBasicParsing | Out-Null
    throw 'POST /api/bot/start unexpectedly succeeded'
  } catch [System.Net.WebException] {
    $response = $_.Exception.Response
    Assert-That ($null -ne $response) 'POST /api/bot/start failed without an HTTP response'
    Assert-That ([int]$response.StatusCode -eq 404) 'POST /api/bot/start must not exist; the worker starts at boot'
  }
}

function Assert-WebSocketReady {
  $webSocketUrl = $baseUri -replace '^https://', 'wss://' -replace '^http://', 'ws://'
  $socket = [System.Net.WebSockets.ClientWebSocket]::new()
  $cancel = [System.Threading.CancellationTokenSource]::new([TimeSpan]::FromSeconds($TimeoutSeconds))
  try {
    $socket.ConnectAsync([Uri]"$webSocketUrl/ws/events", $cancel.Token).GetAwaiter().GetResult()
    $buffer = New-Object byte[] 4096
    $result = $socket.ReceiveAsync([ArraySegment[byte]]::new($buffer), $cancel.Token).GetAwaiter().GetResult()
    Assert-That ($result.MessageType -eq [System.Net.WebSockets.WebSocketMessageType]::Text) 'websocket did not return a text event'
    $event = [Text.Encoding]::UTF8.GetString($buffer, 0, $result.Count) | ConvertFrom-Json
    Assert-That ($event.type -eq 'connection_ready') 'websocket did not return connection_ready'
    Assert-That ($event.data.simulation -eq $true) 'refusing test: cloud is not in SIMULATION=true'
    Assert-That ([string]$event.data.botState -eq 'running') 'worker is not running autonomously'
  } finally {
    if ($socket.State -eq [System.Net.WebSockets.WebSocketState]::Open) {
      $socket.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, 'test complete', [Threading.CancellationToken]::None).GetAwaiter().GetResult()
    }
    $socket.Dispose()
    $cancel.Dispose()
  }
}

if ([string]::IsNullOrWhiteSpace($AdminPassword)) {
  throw 'Set ADMIN_PASSWORD in the shell or pass -AdminPassword. It is used only to verify frontend-console login.'
}

Write-Host '[1/5] Checking autonomous simulation health...'
$health = Invoke-Api '/api/health'
Assert-That ($health.data.service.active -eq $true) 'backend did not report an active service'
Assert-That ($health.data.trading.simulation -eq $true) 'refusing test: cloud is not in SIMULATION=true'
Assert-That ([string]$health.data.trading.botState -eq 'running') 'worker is not running after boot'

Write-Host '[2/5] Checking console authentication only...'
$login = Invoke-Api '/api/auth/login' -Method POST -Body @{ password = $AdminPassword }
$token = [string]$login.data.token
Assert-That (-not [string]::IsNullOrWhiteSpace($token)) 'login did not return a console session token'

Write-Host '[3/5] Checking public read endpoints...'
$dashboard = Invoke-Api '/api/dashboard'
Assert-That ([string]$dashboard.data.botState -eq 'running') 'dashboard state differs from health state'
$config = Invoke-Api '/api/config'
Assert-That ($config.data.simulation -eq $true) 'config does not report simulation mode'
Assert-That ($config.data.liveTradingEnabled -eq $false) 'live trading is unexpectedly enabled'
foreach ($path in @('/api/tokens', '/api/positions', '/api/trades', '/api/rpcs', '/api/wallet')) { [void](Invoke-Api $path) }

Write-Host '[4/5] Confirming HTTP cannot start the worker...'
Assert-StartRouteDisabled

Write-Host '[5/5] Checking WebSocket event handshake...'
Assert-WebSocketReady

Write-Host 'Cloud E2E passed: console login works, simulation is autonomous, and no state was changed.' -ForegroundColor Green
