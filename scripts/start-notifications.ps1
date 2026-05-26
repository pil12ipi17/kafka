$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$cpDemo = Join-Path $root "cp-demo"
$notificationsCompose = Join-Path $root "docker-compose.notifications.yml"
$preferencesService = Join-Path $root "services/notification-preferences"
$filterService = Join-Path $root "services/notification-filter"
$routerService = Join-Path $root "services/notification-router"
$digestService = Join-Path $root "services/notification-digest"

Push-Location $cpDemo
try {
    $env:COMPOSE_DISABLE_ENV_FILE = "1"
    $env:REPOSITORY = "confluentinc"
    $env:CONNECTOR_VERSION = "8.1.0"
    $env:CONTROL_CENTER_KSQL_WIKIPEDIA_URL = "http://ksqldb-server:8088"
    $env:CONTROL_CENTER_KSQL_WIKIPEDIA_ADVERTISED_URL = "http://localhost:8088"
    $env:SSL_CIPHER_SUITES = "TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256,TLS_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
    $env:NOTIFICATION_PREFERENCES_DIR = $preferencesService
    $env:NOTIFICATION_FILTER_DIR = $filterService
    $env:NOTIFICATION_ROUTER_DIR = $routerService
    $env:NOTIFICATION_DIGEST_DIR = $digestService

    docker compose --env-file env_files/config.env -f docker-compose.yml -f $notificationsCompose up -d --build notification-preferences-service notification-filter-service notification-router-service notification-digest-service
}
finally {
    Pop-Location
}
