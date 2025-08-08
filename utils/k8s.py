import os

import requests

import utils.logging as log


def update_pod_deletion_cost(deletion_cost: int) -> bool:
    pod_name = os.getenv("POD_NAME")
    k8s_service_host = os.getenv("KUBERNETES_SERVICE_HOST")
    k8s_service_port = os.getenv("KUBERNETES_SERVICE_PORT")
    if not pod_name:
        log.log(
            "This worker does not seem to be running in K8s pod and therefore can not update the deletion cost."
        )
        return False

    patch = [
        {
            "op": "replace",
            "path": "/metadata/annotations",
            "value": {"controller.kubernetes.io/pod-deletion-cost": str(deletion_cost)},
        }
    ]
    try:
        with open(
            "/var/run/secrets/kubernetes.io/serviceaccount/token", "r", encoding="utf8"
        ) as file:
            k8s_token = file.read()
    except FileNotFoundError:
        log.log("Kubernetes token file not found.")
        return False

    headers = {
        "Content-type": "application/json-patch+json",
        "Authorization": f"Bearer {k8s_token}",
    }

    patch_url = f"https://{k8s_service_host}:{k8s_service_port}/api/v1/namespaces/default/pods/{pod_name}"

    try:
        response = requests.patch(
            url=patch_url,
            headers=headers,
            json=patch,
            verify="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
        )
        if response.status_code != 200:
            raise Exception(f"k8s patch failed: {response.status_code}")
        return True
    except Exception as e:
        log.log(str(e))
        return False
