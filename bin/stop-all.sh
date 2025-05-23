#!/bin/bash

set -e

# Navigate to the script's directory
cd "$(dirname "$0")"
cd ..

echo "⚠️  Deleting all Kubernetes resources in ./k8s..."

kubectl delete -f ./k8s/ --ignore-not-found

sleep 20

echo "✅ All specified resources have been deleted."

# Optional: Show remaining pods/services to confirm cleanup
echo "📦 Remaining Pods:"
kubectl get pods

echo "🧩 Remaining Deployments:"
kubectl get deployments

echo "🔗 Remaining Services:"
kubectl get services
