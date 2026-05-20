$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$ksqlFile = Join-Path $PSScriptRoot "telegram-ready.ksql"
$statement = [System.IO.File]::ReadAllText($ksqlFile, [System.Text.Encoding]::UTF8)
$payload = [pscustomobject]@{
    ksql = $statement
    streamsProperties = @{}
} | ConvertTo-Json -Depth 5
$payloadFile = New-TemporaryFile
[System.IO.File]::WriteAllText($payloadFile, $payload, [System.Text.Encoding]::UTF8)

try {
    $response = curl.exe -s -u "ksqlDBUser:ksqlDBUser" `
        -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" `
        -X POST "http://localhost:8088/ksql" `
        --data-binary "@$payloadFile"
    Write-Output $response
    if ($response -match '"error_code"') {
        throw "ksqlDB returned an error while creating WIKIMEDIA_TELEGRAM_READY."
    }
}
finally {
    Remove-Item -LiteralPath $payloadFile -Force
}

Write-Host "`nCreated or verified ksqlDB stream WIKIMEDIA_TELEGRAM_READY."
