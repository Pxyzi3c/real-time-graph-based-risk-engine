#!/bin/bash

CONTAINER_NAME="postgres_db"
PG_HBA_PATH="/var/lib/postgresql/data/pg_hba.conf" # Correct path confirmed from your previous outputs


echo "Choose restart option:"
echo "1. Soft Refresh (only container, preserve data volume)"
echo "2. Hard Refresh (container and data volume)"
read -p "Enter your choice (soft or hard): " choice

case $choice in
    soft)
        echo "Performing Soft Refresh: Stopping and recreating container, preserving data volume."
        docker compose down
        ;;
    hard)
        echo "Performing Hard Restart: Stopping, recreating container, and DELETING data volume."
        echo "WARNING: This will erase all data in the PostgreSQL database!"
        read -p "Are you sure you want to delete the data volume? (y/n): " confirm_delete
        if [[ "$confirm_delete" == "y" ]]; then
            docker compose down -v
        else 
            echo "Hard Restart cancelled. Exiting script."
            exit 1
        fi
        ;;
    *)
        echo "Invalid choice. Exiting script."
        exit 1
        ;;
esac

echo
docker compose up -d

echo
sleep 5

echo
docker ps -f name=${CONTAINER_NAME}

if docker ps -f name=${CONTAINER_NAME} --format "{{.Status}}" | grep -q "Up"; then
    echo "===== CONTAINER ${CONTAINER_NAME} IS UP. DISPLAYING LOGS (last 50 lines) ====="
    docker logs ${CONTAINER_NAME} --tail 50

    echo "===== DISPLAYING CONTENT OF DATABASES ====="
    docker exec -it ${CONTAINER_NAME} psql -U postgres_container_user -d development -c '\l' '\q'

    echo "===== Displaying content of ${PG_HBA_PATH} inside ${CONTAINER_NAME} ====="
    docker exec -it ${CONTAINER_NAME} bash -c "cat ${PG_HBA_PATH}"
else
    echo "===== Container ${CONTAINER_NAME} is NOT running or failed to start. ====="
    echo "===== Displaying full logs for ${CONTAINER_NAME} to diagnose the issue: ====="
    docker logs ${CONTAINER_NAME} # Show all logs if it failed to start
fi

echo "===== Automation script finished ====="