import yaml
import requests
import os
  
# ===== ENV VARIABLES =====
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    raise ValueError("Missing Databricks environment variables")

# ===== HEADERS =====
headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# ===== READ YAML =====
with open("Job_Configuaration.yml", "r") as f:
    config = yaml.safe_load(f)

job_config = config["job"]

# ===== BUILD TASK PAYLOAD =====
tasks = []

for task in job_config["tasks"]:

    task_payload = {
        "task_key": task["task_key"],

        "notebook_task": {
            "notebook_path": task["notebook_task"]["notebook_path"],
            "source": "WORKSPACE",

            "base_parameters": {
                "primary_keys": task["notebook_task"]["base_parameters"].get("primary_keys", ""),
                "timestamp_column": task["notebook_task"]["base_parameters"].get("timestamp_column", "")
            }
        },

        "environment_key": "serverless"
    }

    # Handle dependency
    if "depends_on" in task:
        task_payload["depends_on"] = task["depends_on"]

# ===== JOB PAYLOAD =====
job_payload = {
    "name": job_config["name"],
    "tasks": tasks,
    "environments": [
        {
            "environment_key": "serverless",
            "spec": {
                "environment_version": "2"
            }
        }
    ]
}

# ===== CHECK IF JOB EXISTS =====
list_response = requests.get(
    f"{DATABRICKS_HOST}/api/2.1/jobs/list",
    headers=headers
)

jobs = list_response.json().get("jobs", [])

existing_job = None

for job in jobs:
    if job["settings"]["name"] == job_config["name"]:
        existing_job = job
        break

# ===== CREATE OR UPDATE JOB =====
if existing_job is None:

    print("Creating job...")

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/create",
        headers=headers,
        json=job_payload
    )

else:

    print("Updating existing job...")

    reset_payload = {
        "job_id": existing_job["job_id"],
        "new_settings": job_payload
    }

    response = requests.post(
        f"{DATABRICKS_HOST}/api/2.1/jobs/reset",
        headers=headers,
        json=reset_payload
    )

print(response.json())
