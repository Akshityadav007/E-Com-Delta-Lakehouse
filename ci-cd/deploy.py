import json
import sys
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi


def load_config():
    with open("config/databricks_config.json") as f:
        return json.load(f)


def build_job_definition(wf):
    """
    Build job payload (same for create and reset)
    """
    return {
        "name": wf["name"],
        "tasks": [
            {
                "task_key": "main",
                "notebook_task": {"notebook_path": wf["notebook"]},
                "job_cluster_key": "job_cluster"
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "job_cluster",
                "new_cluster": {
                    "spark_version": "14.3.x-scala2.12",
                    "node_type_id": wf["node_type"],
                    "num_workers": wf["workers"]
                }
            }
        ]
    }


def deploy(api_client, workflows):
    jobs_api = JobsApi(api_client)

    existing_jobs = {
        job["settings"]["name"]: job
        for job in jobs_api.list_jobs().get("jobs", [])
    }

    for wf in workflows:
        name = wf["name"]
        update = wf.get("update", True)
        payload = build_job_definition(wf)

        # CREATE
        if name not in existing_jobs:
            jobs_api.create_job(payload)
            print(f"[CREATED] {name}")
            continue

        # UPDATE
        if update:
            jobs_api.reset_job(
                existing_jobs[name]["job_id"],
                {"new_settings": payload}  
            )
            print(f"[UPDATED] {name}")
        else:
            print(f"[SKIPPED] {name}")


if __name__ == "__main__":
    config = load_config()
    workflows = config["workflows"]

    api_client = ApiClient(
        host=sys.argv[1],
        token=sys.argv[2]
    )

    deploy(api_client, workflows)
