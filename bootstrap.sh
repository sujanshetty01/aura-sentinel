#!/bin/bash
set -e

echo "========================================================="
echo "🔱 AURA SENTINEL — EC2 Node Bootstrap Script"
echo "========================================================="

# 1. Update system & install prerequisites
echo "[+] Installing system prerequisites..."
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg lsb-release jq git unzip python3-pip

# 2. Install Docker
if ! command -v docker &> /dev/null; then
    echo "[+] Installing Docker Engine..."
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update -y
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
    
    # Add user to docker group
    sudo usermod -aG docker $USER
    echo "[!] Docker installed. You may need to log out and log back in for group changes to take full effect."
else
    echo "[✓] Docker is already installed."
fi

# 3. Install Kubectl
if ! command -v kubectl &> /dev/null; then
    echo "[+] Installing kubectl..."
    sudo snap install kubectl --classic
else
    echo "[✓] kubectl already installed."
fi

# 4. Install Minikube
if ! command -v minikube &> /dev/null; then
    echo "[+] Installing Minikube..."
    curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
    sudo install minikube-linux-amd64 /usr/local/bin/minikube
    rm minikube-linux-amd64
else
    echo "[✓] minikube already installed."
fi

# 5. Start Minikube with resources (2 CPUs, 4GB RAM)
echo "[+] Starting Minikube cluster (2 CPUs, 4096MB Memory)..."
sg docker -c "minikube start --cpus 2 --memory 4096 --driver=docker"

# 6. Build Docker Images
echo "[+] Building internal Python Consumer Docker images..."
sg docker -c 'eval $(minikube docker-env) && \
  docker build -t aura-sentinel/consumer-intelligence:latest -f Dockerfile.intelligence . && \
  docker build -t aura-sentinel/consumer-graph:latest -f Dockerfile.graph . && \
  docker build -t aura-sentinel/metrics-exporter:latest -f Dockerfile.metrics .'

# 7. Install Python Requirements
echo "[+] Installing local Python dependencies..."
pip3 install -r requirements.txt --break-system-packages

# 8. Deploy Kubernetes Stack
echo "[+] Applying Kustomize resources to Minikube..."
sg docker -c "kubectl apply -k k8s/base"

echo "[+] Waiting for essential pods to be ready (this may take a few minutes)..."
sg docker -c "kubectl wait --namespace aura-sentinel --for=condition=ready pod -l app=grafana --timeout=300s"
sg docker -c "kubectl wait --namespace aura-sentinel --for=condition=ready pod -l app=kafka --timeout=300s"

echo "[+] Setting up Port Forwards in the background..."
pkill -f "kubectl port-forward" || true
nohup sg docker -c "kubectl port-forward --address 0.0.0.0 svc/grafana 3000:3000 -n aura-sentinel" > grafana_pf.log 2>&1 &
nohup sg docker -c "kubectl port-forward --address 0.0.0.0 svc/kafka 9092:9092 -n aura-sentinel" > kafka_pf.log 2>&1 &
nohup sg docker -c "kubectl port-forward --address 0.0.0.0 svc/metrics-exporter 8000:8000 -n aura-sentinel" > metrics_pf.log 2>&1 &

echo "[+] Starting synthetic data generator..."
pkill -f "producer.py" || true
# wait a few seconds for port forwards to initialize
sleep 3
nohup python3 producer.py > producer.log 2>&1 &

PUBLIC_IP=$(curl -s ifconfig.me)
if [ -z "$PUBLIC_IP" ]; then
    PUBLIC_IP="127.0.0.1"
fi

echo "========================================================="
echo "✅ Aura Sentinel Pipeline is fully successfully deployed!"
echo "========================================================="
echo ""
echo "📊 Grafana Dashboard: http://$PUBLIC_IP:3000"
echo "   Email: admin"
echo "   Password: AuraSentinel2024"
echo ""
echo "📝 Background processes running:"
echo "   - Grafana port-forward (3000)"
echo "   - Kafka port-forward (9092)"
echo "   - Data Producer (streaming network_flows.csv to Kafka)"
echo ""
echo "Check producer logs with: tail -f producer.log"
echo "========================================================="
