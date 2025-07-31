import requests

from utils import version


def main():
    vqurl = "https://api.345.global"
    vqkey = "ze9BOS091EGUffol"
    job_url = f"{vqurl}/api/v1/jobs/job"
    headers = {
        "X-API-KEY": vqkey,
        "User-Agent": f"pdfmerge/{version.get_git_short_hash()}",
    }

    input_file_uuids = [
        "c8691e4b-16bb-4225-b8ce-6bff3cf70448",
        "45e318ee-294d-479f-9774-a2c8a298fe65",
    ]
    output_folder_uuid = "efb2fcdd-a5f6-4f8f-93fb-66d710228fcc"
    org_uuid = "5471ef92-5c66-4355-88fe-b33a9cebda09"

    data = {
        "tokenLimitation": "",
        "tasks": [
            {
                "id": "0",
                "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NTQwNDkzMDEsInN1YiI6IjgwIiwibm9uY2UiOiJlYzQ2NDkxYy1jNTQ1LTQxYTUtYjhhNS0wMTM3ZDliMzk3YmUifQ.QWlgHLrv1e9YbjvzAazadobSmsphHv8fY9tqeYi1UdM",
                "service": "pdf-merge",
                "majorVersion": 0,
                "retryInterval": 1,
                "retryCount": 1,
                "configuration": {
                    "filesToMerge": input_file_uuids,
                    "destinationFolder": output_folder_uuid,
                    "outputName": "test.pdf",
                    "organisationUuid": org_uuid,
                },
                "shouldFailSiblingTasks": False,
            }
        ],
    }

    response = requests.post(job_url, headers=headers, json=data)
    response.raise_for_status()

    print(response.json())


main()
