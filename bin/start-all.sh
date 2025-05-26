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
docker build -t central-station:latest .

# Build WeatherStation image
echo "➡️ Building WeatherStation image..."
docker build -t weather-station:latest ./WeatherStation

# Build ParqueConverter image
echo "➡️ Building ParqueConverter image..."
docker build -t parque-to-elastic:latest ./ParquetConverter

# Build WeatherStation image
echo "➡️ Building Kafka Processor image..."
docker build -t kafka-processor:latest ./Processor

echo "📦 Applying Kubernetes manifests..."

# Apply all YAML files in the k8s directory
kubectl apply -f ./k8s/weather-stations-deployment.yaml
kubectl apply -f ./k8s/kafka-service.yaml
kubectl apply -f ./k8s/kafka-deployment.yaml
kubectl apply -f ./k8s/central-station-deployment.yaml
kubectl apply -f ./k8s/central-station-service.yaml

echo "✅ Deployment complete."

echo "🌐 Launching Minikube dashboard..."
minikube dashboard