#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

# Navigate to the project directory
cd "$(dirname "$0")"
cd ..

echo "🔧 Configuring Docker to use Minikube's Docker daemon..."
eval $(minikube docker-env)

echo "🐳 Building Docker images within Minikube's Docker environment..."

# Build CentralStation image
echo "➡️ Building CentralStation image..."
docker build -t central-station:latest ./CentralStation

# Build WeatherStation image
echo "➡️ Building WeatherStation image..."
docker build -t weather-station:latest ./WeatherStation

# Build ParqueConverter image
echo "➡️ Building ParqueConverter image..."
docker build -t parque-to-elastic:latest ./ParquetConverter

echo "📦 Applying Kubernetes manifests..."

# Apply all YAML files in the k8s directory
cd k8s

kubectl apply -f weather-stations-deployment.yaml
kubectl apply -f central-station-deployment.yaml
kubectl apply -f kafka-service.yaml
kubectl apply -f kafka-deployment.yaml


echo "✅ Deployment complete."

echo "🌐 Launching Minikube dashboard..."
minikube dashboard