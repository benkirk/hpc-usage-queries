#!/bin/bash

# k8s_cmds.sh - Exercise informational K8s commands for the nwc1 cluster
# This script provides a read-only overview of resources across all namespaces.

# Colors for better readability
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Kubernetes Cluster Overview: nwc1 ===${NC}"

# 1. Ensure we are in the correct context
current_context=$(kubectl config current-context)
if [ "$current_context" != "nwc1" ]; then
    echo -e "${YELLOW}Switching context to nwc1...${NC}"
    kubectl config use-context nwc1
fi

echo -e "\n${GREEN}--- 1. Cluster Info & Nodes ---${NC}"
kubectl cluster-info
kubectl get nodes -o wide

echo -e "\n${GREEN}--- 2. Global Storage (PVs & StorageClasses) ---${NC}"
kubectl get pv
kubectl get sc

echo -e "\n${GREEN}--- 3. Resources Across ALL Namespaces (-A) ---${NC}"
echo -e "${YELLOW}Listing all Pods...${NC}"
kubectl get pods -A

echo -e "\n${YELLOW}Listing all Services (LoadBalancers/ClusterIPs)...${NC}"
kubectl get svc -A

echo -e "\n${YELLOW}Listing all PersistentVolumeClaims...${NC}"
kubectl get pvc -A

echo -e "\n${GREEN}--- 4. Helm Releases (All Namespaces) ---${NC}"
if command -v helm &> /dev/null; then
    helm list -A
else
    echo "Helm not found in PATH."
fi

echo -e "\n${GREEN}--- 5. Configuration Summary (Names & Count) ---${NC}"
# We just count them to keep the output clean
for ns in $(kubectl get ns -o jsonpath='{.items[*].metadata.name}'); do
    secret_count=$(kubectl get secrets -n "$ns" --no-headers 2>/dev/null | wc -l)
    cm_count=$(kubectl get cm -n "$ns" --no-headers 2>/dev/null | wc -l)
    echo -e "Namespace: ${BLUE}$ns${NC} | Secrets: $secret_count | ConfigMaps: $cm_count"
done

echo -e "\n${GREEN}=== Overview Complete ===${NC}"
echo "To troubleshoot a specific pod, use: kubectl describe pod <pod-name> -n <namespace>"
echo "To view logs, use: kubectl logs <pod-name> -n <namespace>"
