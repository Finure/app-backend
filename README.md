# Finure App Backend

## 1. Overview
Finure app backend is designed as the backend for the Finure app. It sets up the database schema using Flyway and starts ingesting credit card application data provided by the end users from Kafka, processes it, sends it to Kserve for predictions from a custom trained model and stores credit card application data along with the prediction in a PostgreSQL database. The application is intended to run as a Kubernetes Deployment, leveraging Helm charts for deployment and configuration. 

## 2. Features
- **Database Schema Setup:** Uses Flyway to automatically set up and migrate the PostgreSQL database schema on startup
- **Credit Application Ingestion:** Listens to Kafka for incoming credit card application data from end users
- **Model Prediction via KServe:** Sends ingested data to KServe for real-time predictions using a custom-trained model
- **Result Storage:** Stores both the original application data and prediction results in PostgreSQL for auditing and analytics
- **Kubernetes Native:** Runs as a Deployment in a Kubernetes cluster, with Helm charts for deployment and configuration
- **Environment Configuration:** Supports environment-specific values for flexible deployments
- **Istio Integration:** Optional script for service mesh setup and graceful sidecar exit

## 3. Prerequisites
- Kubernetes cluster bootstrapped ([Finure Terraform](https://github.com/finure/terraform))
- Infrastructure setup via Flux ([Finure Kubernetes](https://github.com/finure/kubernetes))

If running locally for development/testing:
- Docker
- Python 3.12+ 
- PostgreSQL database 
- Kafka broker 
- KServe deployed 

## 4. File Structure
```
app-backend/
├── app/
│   ├── consume_data.py                 # Kafka consumer and backend logic
│   └── requirements.txt                # Python dependencies
├── flyway/
│   └── V1__init.sql                    # Flyway DB migration script
├── k8s/
│   ├── environments/
│   │   └── production/
│   │       └── values.yaml             # Production environment values handled by CI/CD
│   ├── helm-charts/
│   │   └── app-backend/
│   │       ├── .helmignore
│   │       ├── Chart.yaml              # Helm chart metadata
│   │       ├── values.yaml             # Default Helm values
│   │       └── templates/
│   │           ├── _helpers.tpl        # Helm template helpers
│   │           ├── deployment.yaml     # Kubernetes Deployment manifest
│   │           ├── hpa.yaml            # Horizontal Pod Autoscaler
│   │           └── serviceaccount.yaml # Service account for backend
│   └── scripts/
│       ├── istio.sh                    # Istio setup script
│       └── probe.sh                    # Readiness/Liveness probe script
├── Dockerfile                          # Container build file
├── .dockerignore                       # Docker ignore rules
├── .gitignore                          # Git ignore rules
├── .helmignore                         # Helm ignore rules
├── README.md                           # Project documentation
```

## 5. How to Run Manually

> **Note:** Manual execution is for development/testing only. Production use is via Kubernetes Deployment.

1. Install Python dependencies:
	```bash
	cd app-backend/app
	pip install -r requirements.txt
	```
2. Set required environment variables (see code for details):
	- `KAFKA_BOOTSTRAP_SERVERS` (Kafka connection)
	- `POSTGRES_URI` (PostgreSQL connection)
	- `KSERVE_ENDPOINT` (KServe prediction endpoint)
	- Any Flyway migration configs if running DB setup locally
	- Other required secrets/configs for Kafka, DB, and KServe
3. Run the backend service:
	```bash
	python main.py
	```
	This will:
	- Run Flyway migrations to set up the database schema
	- Start the Kafka consumer to ingest application data
	- Send data to KServe for predictions
	- Store results in PostgreSQL

## 6. k8s Folder Significance

The `k8s` folder contains all Kubernetes-related resources:
- **Helm Charts:** Used to deploy the backend as a Kubernetes Deployment in the cluster. Not intended for standalone or local use.
- **Environment Values:** Customize deployments for different environments (e.g., production)
- **Scripts:** Utility scripts for cluster setup (e.g., Istio service mesh, readiness/liveness probes)

> **Important:** The resources in `k8s` are designed to be consumed by the Kubernetes cluster during automated deployments. They are not meant for manual execution outside the cluster context.

## Additional Information

This repo is primarily designed to be used in the Finure project. While the backend can be adapted for other use cases, it is recommended to use it as part of the Finure platform for full functionality and support.