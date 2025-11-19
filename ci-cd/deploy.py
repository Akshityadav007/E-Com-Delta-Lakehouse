import json
import sys
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

def load_config():
    with open("config/databricks_config.json") as f:
        return json.load(f)

def deploy(api_client, workflows):
    jobs_api = JobsApi(api_client)

    existing = { j["settings"]["name"]: j for j in jobs_api.list_jobs().get("jobs", []) }

    for wf in workflows:
        name = wf["name"]
        update = wf.get("update", True)

        if name in existing:
            if update:
                jobs_api.reset_job(existing[name]["job_id"], {
                    "name": name,
                    "tasks": [{
                        "task_key": "main",
                        "notebook_task": {"notebook_path": wf["notebook"]},
                        "existing_cluster_id": wf["cluster"]
                    }]
                })
                print(f"[UPDATED] {name}")
            else:
                print(f"[SKIPPED] {name}")
        else:
            jobs_api.create_job({
                "name": name,
                "tasks": [{
                    "task_key": "main",
                    "notebook_task": {"notebook_path": wf["notebook"]},
                    "existing_cluster_id": wf["cluster"]
                }]
            })
            print(f"[CREATED] {name}")

if __name__ == "__main__":
    config = load_config()
    workflows = config["workflows"]

    api_client = ApiClient(
        host=sys.argv[1],
        token=sys.argv[2]
    )
    deploy(api_client, workflows)
