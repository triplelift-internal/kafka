variable "docker_image" {
  type = string
  default = "triplelift/redpanda-connect:3.6.2-tl-test-001"
  #default = "triplelift/redpanda-connect:prod-909af88afbcfa47bfadca7aa3fdfef275d92741b"
}

variable "nomad_environment" {
  type = string
  validation {
    condition     = var.nomad_environment == "development" || var.nomad_environment == "production" || var.nomad_environment == "production_eu"
    error_message = "The nomad_environment value must be either development or production."
  }
  default = "development"
}

locals {
  env_name = {
    development   = "dev"
    production    = "prod"
    production_eu = "prod"
  }

  region = {
    development   = "us-east-1"
    production    = "us-east-1"
    production_eu = "eu-central-1"
  }

  consul_datacenter = {
    development   = "aws-us-east-1-dev-ssa-triplelift-net"
    production    = "aws-us-east-1-prod-ssa-triplelift-net"
    production_eu = "aws-eu-central-1-prod-ssa-triplelift-net"
  }

  nomad_datacenter = {
    development   = "aws-dev-ssa-triplelift-net"
    production    = "aws-prod-ssa-triplelift-net"
    production_eu = "aws-prod-ssa-triplelift-net"
  }

  jvm_opts = {
    development   = {
      heap              = "-Xms24G -Xmx24G"   # -Xms = -Xmx (prevents heap resizing overhead)
      ParallelGCThreads = 4                   # match number of vCPUs
      ConcGCThreads     = 2
      G1HeapRegionSize  = "16M"
      MaxGCPauseMillis  = 500
    }
    production    = {
      heap              = "-Xms192G -Xmx192G" # -Xms = -Xmx (prevents heap resizing overhead)
      ParallelGCThreads = 32                  # match number of vCPUs
      ConcGCThreads     = 4
      G1HeapRegionSize  = "32M"
      MaxGCPauseMillis  = 500
    }
    production_eu = {
      heap              = "-Xms96G -Xmx96G"  # -Xms = -Xmx (prevents heap resizing overhead)
      ParallelGCThreads = 16                 # match number of vCPUs
      ConcGCThreads     = 2
      G1HeapRegionSize  = "32M"
      MaxGCPauseMillis  = 500
    }
  }
}


job "redpanda-connect" {
  region = "${local.region[var.nomad_environment]}"

  datacenters = ["${local.nomad_datacenter[var.nomad_environment]}"]

  type = "system"

  vault {
    policies = ["redpanda-connect-secret-reader"]
  }

  group "redpanda-connect" {

    constraint {
      attribute = "${meta.application}"
      value     = "redpanda-connect"
    }

    network {
      mode = "host"

      port "http" {
        static = 8083
        to     = 8083
      }

      port "metrics" {
        static = 4083
        to     = 4083
      }
    }

    task "redpanda-connect" {
      service {
        name = "redpanda-connect"
        tags = ["http"]
        port = "http"

        check {
          name     = "Redpanda Connect Healthcheck"
          type     = "tcp"
          interval = "60s"
          timeout  = "45s"
        }
      }

      service {
        name = "redpanda-connect-jmx-prometheus"
        tags = ["http", "application-metrics"]
        port = "metrics"
      }

      driver = "docker"

      # Graceful shutdown: send SIGTERM and wait 110s before SIGKILL
      kill_signal  = "SIGTERM"
      kill_timeout = "110s"  # Slightly more than CONNECT_TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS (100s)

      resources {
        memory = var.nomad_environment == "production" || var.nomad_environment == "production_eu" ? 240000 : 15000
      }

      config {
        image        = var.docker_image
        network_mode = "host"
        privileged   = true
        ports        = ["http", "metrics"]

        logging {
          type = "awslogs"
          config {
            awslogs-region       = "${local.region[var.nomad_environment]}"
            awslogs-group        = "redpanda-connect-${local.env_name[var.nomad_environment]}"
            awslogs-create-group = true
            awslogs-stream       = "${meta.instance-id}"
          }
        }
      }

      template {
        data        = <<EOF
# Reduced from 300s to complete rebalances before multiple spot terminations can cascade
CONNECT_REBALANCE_TIMEOUT_MS="120000"

# Must be shorter than session timeout to avoid premature session expiration
CONNECT_REQUEST_TIMEOUT_MS="40000"

# Reduced for faster rebalancing in spot instance environments
CONNECT_SCHEDULED_REBALANCE_MAX_DELAY_MS="30000"

# Spot instance optimization: 30s session timeout enables fast failure detection (vs 2-min spot termination notice)
CONNECT_SESSION_TIMEOUT_MS="30000"

# Heartbeat interval should be ~1/3 of session timeout to ensure multiple heartbeats within timeout window
CONNECT_HEARTBEAT_INTERVAL_MS="10000"

# Max time between poll() calls; prevents false failures during heavy processing while staying under rebalance timeout
CONNECT_MAX_POLL_INTERVAL_MS="120000"

# Increased from 30s to 100s to utilize full 2-minute spot termination notice for clean offset flushing
CONNECT_TASK_SHUTDOWN_GRACEFUL_TIMEOUT_MS="100000"

# Reduced to align with faster rebalancing expectations
CONNECT_WORKER_SYNC_TIMEOUT_MS="45000"

CONNECT_ENABLE_GLOBAL_BALANCE_TASK_ASSIGNOR=true
CONNECT_ADMIN_OVERRIDE_FETCH_MAX_BYTES="4194304"
CONNECT_ADMIN_OVERRIDE_FETCH_MAX_WAIT_MS="500"
CONNECT_ADMIN_OVERRIDE_MAX_PARTITION_FETCH_BYTES="4194304"
CONNECT_ADMIN_OVERRIDE_MAX_POLL_RECORDS="1000"
CONNECT_ADMIN_OVERRIDE_RECEIVE_BUFFER_BYTES="25165824"
CONNECT_BOOTSTRAP_SERVERS="{{range $index, $service := service "redpanda@${local.consul_datacenter[var.nomad_environment]}"}}{{if ne $index 0}},{{end}}SASL_PLAINTEXT://{{$service.Address}}:9092{{end}}"
CONNECT_CONFIG_STORAGE_TOPIC="redpanda-connect.${local.env_name[var.nomad_environment]}.config"
CONNECT_CONNECT_PROTOCOL="sessioned"
CONNECT_CONNECTIONS_MAX_IDLE_MS="30000"
CONNECT_CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY="All"
CONNECT_CONSUMER_FETCH_MAX_BYTES="4194304"
CONNECT_CONSUMER_FETCH_MAX_WAIT_MS="500"
CONNECT_CONSUMER_MAX_PARTITION_FETCH_BYTES="4194304"
CONNECT_CONSUMER_MAX_POLL_RECORDS="1000"
CONNECT_CONSUMER_OVERRIDE_FETCH_MAX_BYTES="4194304"
CONNECT_CONSUMER_OVERRIDE_FETCH_MAX_WAIT_MS="500"
CONNECT_CONSUMER_OVERRIDE_MAX_PARTITION_FETCH_BYTES="4194304"
CONNECT_CONSUMER_OVERRIDE_MAX_POLL_RECORDS="1000"
CONNECT_CONSUMER_OVERRIDE_PARTITION_ASSIGNMENT_STRATEGY="org.apache.kafka.clients.consumer.CooperativeStickyAssignor, org.apache.kafka.clients.consumer.StickyAssignor, org.apache.kafka.clients.consumer.RoundRobinAssignor"
CONNECT_CONSUMER_OVERRIDE_RECEIVE_BUFFER_BYTES="25165824"
CONNECT_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY="org.apache.kafka.clients.consumer.CooperativeStickyAssignor, org.apache.kafka.clients.consumer.StickyAssignor, org.apache.kafka.clients.consumer.RoundRobinAssignor"
CONNECT_CONSUMER_RECEIVE_BUFFER_BYTES="25165824"
CONNECT_CONSUMER_SASL_JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{with secret "secret/data/infra/redpanda-connect/redpanda"}}{{.Data.data.connect_redpanda_user}}{{end}}\" password=\"{{with secret "secret/data/infra/redpanda-connect/redpanda"}}{{.Data.data.connect_redpanda_password}}{{end}}\";"
CONNECT_CONSUMER_SASL_MECHANISM="SCRAM-SHA-256"
CONNECT_CONSUMER_SECURITY_PROTOCOL="SASL_PLAINTEXT"
CONNECT_CONSUMER_FETCH_MIN_BYTES="1048576"
CONNECT_FETCH_MAX_BYTES="4194304"
CONNECT_FETCH_MAX_WAIT_MS="500"
CONNECT_GROUP_ID="redpanda-connect-${local.env_name[var.nomad_environment]}"
CONNECT_INTERNAL_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
CONNECT_INTERNAL_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
CONNECT_KEY_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
CONNECT_LOG4J_ROOT_LOGLEVEL="INFO"
CONNECT_MASTER_ELIGIBILITY="true"
CONNECT_MAX_PARTITION_FETCH_BYTES="4194304"
CONNECT_MAX_POLL_RECORDS="1000"
CONNECT_METADATA_MAX_AGE_MS="300000"
CONNECT_OFFSET_FLUSH_INTERVAL_MS="60000"
CONNECT_OFFSET_FLUSH_TIMEOUT_MS="30000"
CONNECT_OFFSET_STORAGE_PARTITIONS="168"
CONNECT_OFFSET_STORAGE_TOPIC="redpanda-connect.${local.env_name[var.nomad_environment]}.offsets"
CONNECT_RECEIVE_BUFFER_BYTES="25165824"
CONNECT_REST_ADVERTISED_PORT="8083"
CONNECT_REST_EXTENSION_CLASSES="org.apache.kafka.connect.rest.basic.auth.extension.BasicAuthSecurityRestExtension"
CONNECT_REST_HOST_NAME="0.0.0.0"
CONNECT_REST_PORT="8083"
CONNECT_SASL_JAAS_CONFIG="org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{{with secret "secret/data/infra/redpanda-connect/redpanda"}}{{.Data.data.connect_redpanda_user}}{{end}}\" password=\"{{with secret "secret/data/infra/redpanda-connect/redpanda"}}{{.Data.data.connect_redpanda_password}}{{end}}\";"
CONNECT_SASL_MECHANISM="SCRAM-SHA-256"
CONNECT_SECURITY_PROTOCOL="SASL_PLAINTEXT"
CONNECT_STATUS_STORAGE_PARTITIONS="168"
CONNECT_STATUS_STORAGE_TOPIC="redpanda-connect.${local.env_name[var.nomad_environment]}.status"
CONNECT_VALUE_CONVERTER="org.apache.kafka.connect.json.JsonConverter"
CONNECT_WORKER_UNSYNC_BACKOFF_MS="60000"
KAFKA_HEAP_OPTS="${local.jvm_opts[var.nomad_environment].heap}"
KAFKA_JMX_PORT="4080"
KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:ParallelGCThreads=${local.jvm_opts[var.nomad_environment].ParallelGCThreads} -XX:ConcGCThreads=${local.jvm_opts[var.nomad_environment].ConcGCThreads} -XX:G1HeapRegionSize=${local.jvm_opts[var.nomad_environment].G1HeapRegionSize} -XX:MaxGCPauseMillis=${local.jvm_opts[var.nomad_environment].MaxGCPauseMillis} -XX:InitiatingHeapOccupancyPercent=45 -XX:+ExplicitGCInvokesConcurrent -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Djava.awt.headless=true -Djava.library.path=/usr/lib64"
KAFKA_OPTS="-javaagent:/etc/kafka-connect/jmx_prometheus_javaagent.jar=4083:/etc/kafka-connect/prometheus.yaml -Djava.security.auth.login.config=/etc/kafka-connect/rest-api-auth.conf"
PORT="8083"
RESTAPI_CREDENTIALS="{{with secret "secret/data/infra/redpanda-connect/cluster"}}{{.Data.data.RESTAPI_CREDENTIALS}}{{end}}"
CONNECT_CONFIG_PROVIDERS=env
CONNECT_CONFIG_PROVIDERS_ENV_CLASS=org.apache.kafka.common.config.provider.EnvVarConfigProvider
PLATFORM_CHANGELOG_OPENSEARCH_USERNAME={{with secret "secret/data/infra/redpanda-connect/opensearch"}}{{.Data.data.PLATFORM_CHANGELOG_OPENSEARCH_USERNAME}}{{end}}
PLATFORM_CHANGELOG_OPENSEARCH_PASSWORD={{with secret "secret/data/infra/redpanda-connect/opensearch"}}{{.Data.data.PLATFORM_CHANGELOG_OPENSEARCH_PASSWORD}}{{end}}
AWS_RETRY_MODE="standard"
AWS_MAX_ATTEMPTS="5"
EOF
        destination = "/secrets/file.env"
        env         = true
        change_mode = "noop"
      }
    }
  }
}
