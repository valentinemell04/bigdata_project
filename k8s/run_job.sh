#!/usr/bin/env bash
# Lance le pipeline sous forme de Job Kubernetes depuis le terminal du service vscode-pyspark.
# Prérequis : service lancé avec le rôle Kubernetes « admin » (formulaire Onyxia, section Role),
# sinon kubectl n'a pas le droit de créer des Jobs.
# Usage (depuis la racine du dépôt) : bash k8s/run_job.sh
set -euo pipefail
NS=${KUBERNETES_NAMESPACE}
REPO_URL=${REPO_URL:-https://github.com/valentinemell04/bigdata_project.git}

echo "== $(date '+%Y-%m-%d %H:%M:%S %Z') | namespace : ${NS}"

# 1. Même image que le service en cours (on la lit sur notre propre Pod)
IMAGE=$(kubectl get pod "$HOSTNAME" -n "$NS" -o jsonpath='{.spec.containers[0].image}')
echo "image : ${IMAGE}"

# 2. Secret avec les clés S3 temporaires du service (cours 2 : Secret -> variables d'environnement)
kubectl delete secret dvf-s3-credentials -n "$NS" --ignore-not-found
kubectl create secret generic dvf-s3-credentials -n "$NS" \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="${AWS_SESSION_TOKEN:-}" \
  --from-literal=AWS_S3_ENDPOINT="$AWS_S3_ENDPOINT" \
  --from-literal=AWS_ENDPOINT_URL="https://${AWS_S3_ENDPOINT}" \
  --from-literal=AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# 3. Un Job ne se relance pas : on supprime l'ancien avant d'appliquer le nouveau
kubectl delete job dvf-pipeline -n "$NS" --ignore-not-found
sed -e "s|\${IMAGE}|${IMAGE}|" -e "s|\${REPO_URL}|${REPO_URL}|" k8s/job-pipeline.yaml | kubectl apply -n "$NS" -f -

# 4. Suivre l'exécution
kubectl wait --for=condition=Ready pod -l app=dvf-pipeline -n "$NS" --timeout=600s || true
kubectl logs -f job/dvf-pipeline -n "$NS"
kubectl get job dvf-pipeline -n "$NS"
kubectl get pods -l app=dvf-pipeline -n "$NS"
