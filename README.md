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

**Login Credentials** | 
    Username: `Admin`
    Password: `Admin`

1.  **Network Initialization & Baseline:** Open `index.html` and log in. Navigate to the **Network & Liquidity** tab. Note the baseline state: all branches are stable, and the Lamport Clocks for each branch are synchronized at their starting integers.
2.  **Liquidity Warning Simulation:** Click the **Manage ▾** dropdown on Branch 101 and click **Withdraw $10,000**. The UI will instantly react, turning the branch card red with a `Liquidity Warning` badge to indicate the branch has dropped below its minimum operating threshold.
3.  **Distributed Wire Transfer (Clock Sync):** To bail out Branch 101, open the dropdown for Branch 102 and click **Transfer $10,000**. Watch the Lamport Clocks for *both* branches increment simultaneously, proving that the centralized HQ is correctly ordering the distributed cross-node event. Branch 101 will now recover its stable state.
4.  **Traffic Spike (High Volume):** We need to simulate a surge at Branch 102. Click **Deposit $10,000** on Branch 102 three times to push its balance to $30,000 (crossing the $25,000 threshold). The UI will dynamically flag the branch with a `HIGH VOLUME` badge, signaling that this node requires scaling.
5.  **Auto-Scaling Dispatch (Greedy Algorithm):** Switch to the **Workforce & HR** tab. Use the "Hire Staff" form to hire a new "Backup" employee. Because Branch 102 is experiencing a traffic spike, the Greedy Dispatch Engine will instantly intercept the new hire and safely route them to Branch 102's Live Weekly Schedule.
6.  **The Algorithmic Tie-Breaker:** Go back to the Network tab. Click **Deposit $10,000** on Branch 101 until its balance hits $40,000. Now *both* branches are over the High Volume threshold, but Branch 101 has the higher severity. Return to the HR tab and hire one more Backup. Watch the algorithm perfectly resolve the tie by routing the backup to Branch 101.
7.  **System Recovery (Database Reset):** To conclude the demo, click the **Reset Network** and **Reset Roster** buttons. The FastAPI backend will safely drop the SQLite tables, re-seed the baseline configurations, and broadcast the reset to the network, snapping the dashboard perfectly back to Step 1.

## Teardown
To shut down the network and remove the containers, run:
```bash
docker-compose down
```
