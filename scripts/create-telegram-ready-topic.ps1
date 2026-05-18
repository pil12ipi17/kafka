$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$ksqlFile = Join-Path $PSScriptRoot "telegram-ready.ksql"
$statement = Get-Content -Raw -Encoding UTF8 $ksqlFile
$payload = @{
    ksql = $statement
    streamsProperties = @{}
} | ConvertTo-Json -Depth 5
$payloadFile = New-TemporaryFile
Set-Content -LiteralPath $payloadFile -Value $payload -Encoding UTF8

try {
    curl.exe -s -u "ksqlDBUser:ksqlDBUser" `
        -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" `
        -X POST "http://localhost:8088/ksql" `
        --data-binary "@$payloadFile"
}
finally {
    Remove-Item -LiteralPath $payloadFile -Force
}

Write-Host "`nCreated or verified ksqlDB stream WIKIMEDIA_TELEGRAM_READY."
