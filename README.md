# Distributed Multi-Branch Bank Management System

This repository contains a full-stack distributed bank management system. It simulates a centralized Headquarters (HQ) node communicating with multiple local Branch nodes over a REST API network to maintain real-time synchronization of critical financial and human resource data.

## Core Features
* **Distributed State Synchronization:** Utilizes Lamport Logical Clocks to ensure event ordering and data consistency across all nodes in the network.
* **Dynamic Auto-Scaling (Greedy Algorithm):** Features a custom HR Dispatch Engine that acts as an auto-scaler. It evaluates network-wide severity scores (missing tellers + transaction volume) every millisecond to dynamically route "Backup" floaters to branches experiencing traffic spikes (balances > $25,000).
* **Hybrid Infrastructure Simulation:** Models both static resources (Dedicated Managers/Tellers) and ephemeral resources (Global Backup Pool) responding to live system load.
* **Interactive Command Center:** A frontend UI dashboard to visualize live scheduling, network state, and execute cross-node transactions.

## Prerequisites
To run this project, you will need the following installed on your machine:
* **Docker Desktop:** Ensure the Docker engine is running.
* **Git:** To clone the repository.
* **Code Editor:** Visual Studio Code or PyCharm.

## Repository Structure
```text
Distributed-Bank-Management-System/
├── docker-compose.yml       
├── index.html               
├── hq_server/
│   ├── database.py           
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── scheduler.py
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
    cd Distributed-Bank-Management-System
    ```

2.  **Build and launch the backend containers:**
    Open your terminal in the root directory and run:
    ```bash
    docker-compose up --build
    ```

3.  **Launch the Frontend Dashboard:**
    Once the containers are running and the `hq-server` is listening on port 8000, simply double-click the `index.html` file in your repository folder to open it in your web browser. 

## How to Test and Verify (Demo Protocol)
To confirm the Headquarters is successfully receiving, aggregating, and routing data across the distributed branches:

1.  **Monitor the State:** Open `index.html` and watch the live logs. You will see branches pinging the HQ with automated transactions and Lamport timestamps.
2.  **Trigger High Volume:** Use the Control Panel to make a large deposit into Branch 101, pushing its balance over $25,000. Notice the UI dynamically flag the branch as `HIGH VOLUME` and open a 5th employee slot.
3.  **Test the Routing Algorithm:** Use the "Hire Staff" form to hire a "Backup" employee. Because Branch 101 crossed the volume threshold, the Greedy Algorithm will instantly intercept the new hire and dispatch them to Branch 101 to handle the load.
4.  **Test the Tie-Breaker:** Push Branch 102 over the threshold as well, but give it a higher balance than Branch 101. Hire another Backup and watch the algorithm prioritize Branch 102 based on the financial tie-breaker logic.

## Teardown
To shut down the network and remove the containers, run:
```bash
docker-compose down
```
