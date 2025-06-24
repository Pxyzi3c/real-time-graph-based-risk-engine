## Problem:

**1. Can't reference docker postgresql for development like putting a data**
**Solution:**
- check ```pg_hba.conf```
    * Access postgres container
    ```bash
    docker exec -it postgres_db bash
    ```
    * Access ```pg_hba.conf```
    ```bash
    cat /var/lib/postgresql/data/pg_hba.conf
    ```
- Look for relevant lines
    * Important lines to check:
    ```bash
    # IPv4 local connections:
    host    all             all             127.0.0.1/32            md5
    # IPv6 local connections:
    host    all             all             ::1/128                 md5
    # For connections from Docker's internal network (if any default):
    host    all             all             0.0.0.0/0               md5
    host    all             all             ::/0                    md5
    ```
    * Ensure the METHOD is md5 or scram-sha-256 (password-based authentication) for both IPv4 and IPv6 localhost entries. If it's trust (no password) or ident (system user must match DB user), that's your problem for localhost connections.