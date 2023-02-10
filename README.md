# airflow-data-reconciliation
In the current implementation, I tried to reproduce a real-life example from my practice where it was necessary to reconcile data based on several sources.

Instruction:
1. Download Docker.
2. Clone this repository.
3. Open a terminal in the repository directory.
4. Run command `docker-compose up -d`
5. Open page **localhost:8080**
6. Enter **airflow** and **airflow** as login and password.
7. Create 2 connections in Admin -> Connections:
   Connection Id: **postgres**
   Connection Type: **Postgres**
   Host: **postgres**
   Login: **airflow**
   Password: **airflow**
   Port: **5432**

   Connection Id: **temp-folder**
   Connection Type: **File (path)**
   Extra: **{"path": "/tmp"}**
8. Run DAG data_reconciliation.
