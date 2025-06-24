#!/bin/bash

CONTAINER_NAME="postgres_db"
PG_HBA_PATH="/var/lib/postgresql/data/pg_hba.conf" # Correct path confirmed from your previous outputs

echo
docker compose down

echo
docker compose up -d

echo
sleep 5

echo
docker ps -f name=${CONTAINER_NAME}

if docker ps -f name=${CONTAINER_NAME} --format "{{.Status}}" | grep -q "Up"; then
    echo "--- CONTAINER ${CONTAINER_NAME} IS UP. DISPLAYING LOGS (last 50 lines) ---"
    docker logs ${CONTAINER_NAME} --tail 50

    echo "--- DISPLAYING CONTENT OF DATABASE ---"
    docker exec -it ${CONTAINER_NAME} psql -U postgres_container_user -d development -c '\l' '\q'

    echo "--- Displaying content of ${PG_HBA_PATH} inside ${CONTAINER_NAME} ---"
    docker exec -it ${CONTAINER_NAME} bash -c "cat ${PG_HBA_PATH}"
else
    echo "--- Container ${CONTAINER_NAME} is NOT running or failed to start. ---"
    echo "--- Displaying full logs for ${CONTAINER_NAME} to diagnose the issue: ---"
    docker logs ${CONTAINER_NAME} # Show all logs if it failed to start
fi

echo "--- Automation script finished ---"