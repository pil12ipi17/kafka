$ErrorActionPreference = "Stop"

$root = Resolve-Path (Join-Path $PSScriptRoot "..")
$cpDemo = Join-Path $root "cp-demo"

function Set-CpDemoEnv {
    $env:COMPOSE_DISABLE_ENV_FILE = "1"
    $env:REPOSITORY = "confluentinc"
    $env:CONNECTOR_VERSION = "8.1.0"
    $env:CONTROL_CENTER_KSQL_WIKIPEDIA_URL = "http://ksqldb-server:8088"
    $env:CONTROL_CENTER_KSQL_WIKIPEDIA_ADVERTISED_URL = "http://localhost:8088"
    $env:SSL_CIPHER_SUITES = "TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256,TLS_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
}

function Invoke-CpCompose {
    docker compose --env-file env_files/config.env -f docker-compose.yml @args
}

function Wait-ContainerHealthy {
    param(
        [string[]] $Names,
        [int] $TimeoutSeconds = 240
    )

    foreach ($name in $Names) {
        $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
        Write-Host "Waiting for $name to become healthy..."
        while ((Get-Date) -lt $deadline) {
            $status = docker inspect $name --format "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" 2>$null
            if ($LASTEXITCODE -eq 0 -and ($status -eq "healthy" -or $status -eq "running")) {
                Write-Host "$name is $status"
                break
            }
            Write-Host "$name status: $status"
            Start-Sleep -Seconds 5
        }

        if ((Get-Date) -ge $deadline) {
            throw "Timed out waiting for $name to become healthy"
        }
    }
}

Push-Location $cpDemo
try {
    Set-CpDemoEnv

    docker build --build-arg CP_VERSION=8.1.0 --build-arg REPOSITORY=confluentinc -t localbuild/connect:8.1.0-8.1.0 -f Dockerfile .

    docker run --rm -v "${PWD}\scripts:/work" -u 0 confluentinc/cp-base-new:8.1.0 bash -c "find /work -type f \( -name '*.sh' -o -name '*.jq' -o -name '*.json' -o -name '*.properties' \) -print0 | xargs -0 sed -i 's/\r$//'"
    docker run --rm -v "${PWD}\scripts\security:/etc/kafka/secrets" -u 0 confluentinc/cp-base-new:8.1.0 bash -c "cd /etc/kafka/secrets && ./certs-clean.sh && ./certs-create.sh && mkdir -p /etc/kafka/secrets/keypair && openssl genrsa -out /etc/kafka/secrets/keypair/keypair.pem 2048 && openssl rsa -in /etc/kafka/secrets/keypair/keypair.pem -outform PEM -pubout -out /etc/kafka/secrets/keypair/public.pem && chmod 644 /etc/kafka/secrets/keypair/keypair.pem /etc/kafka/secrets/*.key"
    docker run --rm -u root -v "${PWD}\scripts\security:/etc/kafka/secrets" localbuild/connect:8.1.0-8.1.0 keytool -importkeystore -srckeystore /usr/lib/jvm/temurin-21-jre/lib/security/cacerts -srcstorepass changeit -destkeystore /etc/kafka/secrets/kafka.connect.truststore.jks -deststorepass confluent -keypass confluent -noprompt

    Invoke-CpCompose up --no-recreate -d openldap tools kafka1 kafka2
    Wait-ContainerHealthy -Names @("kafka1", "kafka2") -TimeoutSeconds 360

    docker exec tools bash -c "cp /etc/kafka/secrets/snakeoil-ca-1.crt /usr/local/share/ca-certificates && /usr/sbin/update-ca-certificates && /tmp/helper/create-role-bindings.sh"
    docker exec kafka1 kafka-configs --bootstrap-server kafka1:12091 --entity-type topics --entity-name _confluent-metadata-auth --alter --add-config min.insync.replicas=1

    Invoke-CpCompose up --no-recreate -d schemaregistry connect control-center ksqldb-server ksqldb-cli
    Wait-ContainerHealthy -Names @("schemaregistry", "connect", "ksqldb-server") -TimeoutSeconds 360

    Invoke-CpCompose up --no-recreate -d elasticsearch kibana
    Wait-ContainerHealthy -Names @("elasticsearch", "kibana") -TimeoutSeconds 360

    docker exec tools bash -c "/tmp/helper/create-topics.sh"
    docker cp scripts/connectors/wikipedia-sse.json connect:/tmp/wikipedia-sse.json
    docker exec connect curl -sS -X POST -H "Content-Type: application/json" --data "@/tmp/wikipedia-sse.json" --cert /etc/kafka/secrets/connect.certificate.pem --key /etc/kafka/secrets/connect.key --tlsv1.2 --cacert /etc/kafka/secrets/snakeoil-ca-1.crt -u connectorSubmitter:connectorSubmitter https://connect:8083/connectors
    docker exec ksqldb-cli bash -c "ksql -u ksqlDBUser -p ksqlDBUser http://ksqldb-server:8088 <<'EOF'
RUN SCRIPT '/tmp/statements.sql';
exit ;
EOF"
    docker cp (Join-Path $root "scripts/elasticsearch-ksqldb.json") connect:/tmp/elasticsearch-ksqldb.json
    docker exec connect curl -sS -X PUT -H "Content-Type: application/json" --data "@/tmp/elasticsearch-ksqldb.json" --cert /etc/kafka/secrets/connect.certificate.pem --key /etc/kafka/secrets/connect.key --tlsv1.2 --cacert /etc/kafka/secrets/snakeoil-ca-1.crt -u connectorSubmitter:connectorSubmitter https://connect:8083/connectors/elasticsearch-ksqldb/config
}
finally {
    Pop-Location
}
