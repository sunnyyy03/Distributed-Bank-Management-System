# Distributed Multi-Branch Bank Management System
This repository contains the foundational Minimum Viable Product (MVP) for our distributed bank management system. It simulates a centralized Headquarters (HQ) node communicating with local Branch nodes over a REST API network to maintain real-time synchronization of critical data, such as cash reserves.

## Prerequisites
To run this project, you will need the following installed on your machine:
* **Docker Desktop:** Ensure the Docker engine is running.
* **Git:** To clone the repository.
* **Code Editor:** Visual Studio Code or PyCharm

## Repository Structure
```text
bank_management_system/
├── docker-compose.yml       
├── hq_server/
│   ├── database.py
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py             
└── branch_node/            
    ├── Dockerfile
    ├── requirements.txt
    └── branch_client.py    
```

## How to Run the Distributed Network
This project uses Docker Compose to simulate a distributed network on your local machine. 

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/sunnyyy03/Distributed-Bank-Management-System.git
    cd bank_management_system
    ```

2.  **Build and launch the containers:**
    Open your terminal in the root directory and run:
    ```bash
    docker-compose up --build
    ```

3.  **Monitor the terminal output:**
    You will see the `hq-server` container boot up the FastAPI application on port 8000. Shortly after, the `branch-node-101` container will boot and begin sending automated HTTP POST requests containing JSON payloads and Lamport timestamps every 5 seconds.

## How to Test and Verify
To confirm the Headquarters is successfully receiving and aggregating the state from the distributed branches:
1.  Open a web browser or termianl.
2.  Navigate to the HQ status endpoint: `http://localhost:8000/status` (or use `curl -s http://localhost:8000/status`)
3.  Refresh the page every few seconds. You should see the `current_balance` updating in real-time, alongside an incrementing `hq_clock` integer representing the Lamport logical time.

## Teardown
To shut down the network and remove the containers, run:
```bash
docker-compose down
```
