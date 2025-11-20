import json
import sys
import requests


def load_config():
    with open("config/databricks_config.json") as f:
        return json.load(f)


def build_job_definition(wf):
    return {
        "name": wf["name"],
        "tasks": [
            {
                "task_key": "main",
                "notebook_task": {
                    "notebook_path": wf["notebook"]
                },
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


def api_call(host, token, method, path, payload=None):
    url = f"{host}/api/2.1{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    resp = requests.request(method, url, headers=headers, json=payload)

    if resp.status_code >= 300:
        raise Exception(f"API Error {resp.status_code}: {resp.text}")

    return resp.json() if resp.text else {}


def deploy(host, token, workflows):
    # 1. List all jobs
    existing_jobs = api_call(host, token, "GET", "/jobs/list").get("jobs", [])
    existing = {job["settings"]["name"]: job for job in existing_jobs}

    for wf in workflows:
        name = wf["name"]
        update = wf.get("update", True)

        payload = build_job_definition(wf)

        if name not in existing:
            # CREATE job
            api_call(host, token, "POST", "/jobs/create", payload)
            print(f"[CREATED] {name}")
            continue

        # UPDATE job
        if update:
            job_id = existing[name]["job_id"]
            api_call(
                host,
                token,
                "POST",
                "/jobs/reset",
                {"job_id": job_id, "new_settings": payload}
            )
            print(f"[UPDATED] {name}")
        else:
            print(f"[SKIPPED] {name}")


if __name__ == "__main__":
    config = load_config()
    workflows = config["workflows"]

    host = sys.argv[1]
    token = sys.argv[2]

    deploy(host, token, workflows)
