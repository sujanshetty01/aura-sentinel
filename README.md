# 🔱 Aura Sentinel

**Aura Sentinel** is an autonomous network telemetry, intelligence, and auto-remediation orchestration stack. It is designed to consume simulated network flows, analyze behavioral intelligence, maintain a graph of network actors, and provide real-time observability.

## 🏗️ Architecture Stack
- **Message Broker:** Kafka + Zookeeper (Real-time telemetry streaming)
- **Database:** Neo4j (Graph mapping of network flows & potential threats)
- **Microservices:** Python (Consumer Graph, Intelligence, and Metrics Exponent)
- **Observability:** Prometheus & Grafana (Real-time dashboards)
- **Remediation:** Terraform (Auto-remediation execution)
- **Orchestration:** Kubernetes (Minikube / Kustomize)

---

## 🚀 Quickstart Guide

For a new user or a fresh environment, starting the entire pipeline requires just **a single command**.

1. Navigate to the project directory:
   ```bash
   cd ~/aura-sentinel
   ```
2. Run the bootstrap script:
   ```bash
   chmod +x bootstrap.sh
   ./bootstrap.sh
   ```

**What this script does behind the scenes:**
1. Installs all required dependencies (Docker, Minikube, Kubectl, Python dependencies).
2. Starts a local Minikube Kubernetes cluster.
3. Builds the internal Docker images for the Python microservices.
4. Deploys the complete Kubernetes stack (Kafka, Neo4j, Prometheus, Grafana, etc.) via Kustomize.
5. Waits for essential services to be fully ready.
6. Automatically establishes background port-forwards so you can access the dashboard.
7. Launches `producer.py` in the background to begin simulating network traffic flow.

---

## 📊 Accessing the Dashboard

Once the `bootstrap.sh` script completes, it will automatically detect your public IP address and print the Grafana access details directly into your terminal output.

- **URL:** `http://<YOUR_PUBLIC_IP>:3000` (or `http://localhost:3000` if viewing locally)
- **Username:** `admin`
- **Password:** `AuraSentinel2024`

> **Note for Remote (EC2) Access:** If you are accessing this from an EC2 instance and your AWS Security Group does not expose port `3000`, you can establish a secure connection using an SSH Tunnel from your local machine:
> `ssh -L 3000:127.0.0.1:3000 ubuntu@<EC2_IP>`
> Then, just open `http://localhost:3000` in your web browser.

---

## 📡 The Data Pipeline

The pipeline is pre-configured to automatically inject structured telemetry once the cluster is initialized.

- **The Data Producer (`producer.py`)**: Runs automatically in the background following the execution of the `bootstrap.sh` script. It extracts traffic records from `network_flows.csv` and publishes them step-by-step to the Kafka `network-telemetry` topic.
- Monitor the live data ingestion by tailing the logs:
  ```bash
  tail -f producer.log
  ```

## 🧹 Maintenance & Cleanup

If you need to halt the background processes or shut down the cluster:

**Stop background port-forwards and the data producer:**
```bash
pkill -f "kubectl port-forward"
pkill -f "producer.py"
```

**Stop the Kubernetes cluster entirely:**
```bash
minikube stop
# Completely remove cluster (optional)
# minikube delete
```
