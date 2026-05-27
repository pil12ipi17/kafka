$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$cpDemo = Join-Path $root "cp-demo"
$telegramCompose = Join-Path $root "docker-compose.telegram.yml"
$telegramDelivery = Join-Path $root "services/telegram-delivery"

Push-Location $cpDemo
try {
    $env:COMPOSE_DISABLE_ENV_FILE = "1"
    $env:REPOSITORY = "confluentinc"
    $env:CONNECTOR_VERSION = "8.1.0"
    $env:CONTROL_CENTER_KSQL_WIKIPEDIA_URL = "http://ksqldb-server:8088"
    $env:CONTROL_CENTER_KSQL_WIKIPEDIA_ADVERTISED_URL = "http://localhost:8088"
    $env:SSL_CIPHER_SUITES = "TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256,TLS_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
    $env:TELEGRAM_DELIVERY_DIR = $telegramDelivery
    if (-not $env:TELEGRAM_RECIPIENTS_CONFIG) {
        $env:TELEGRAM_RECIPIENTS_CONFIG = Join-Path $telegramDelivery "config/recipients.example.json"
    }

    docker compose --env-file env_files/config.env -f docker-compose.yml -f $telegramCompose up -d --build telegram-delivery-service
}
finally {
    Pop-Location
}
