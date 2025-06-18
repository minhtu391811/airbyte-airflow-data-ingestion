docker network create ingestion_network

docker network connect ingestion_network airbyte-abctl-control-plane

docker compose up airflow-init

sleep 5

docker compose up -d

sleep 5