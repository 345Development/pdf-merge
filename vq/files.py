from collections import defaultdict
import os
from pathlib import Path
import shutil
from uuid import UUID

import requests
import vq.api
from vq.jobs_manager import Job
from typing import Any, Dict, List, Optional, Set
import utils.logging as log
from utils import GracefulShutdownHandler
import concurrent.futures


class DownloadManager:
    def __init__(self, total: Optional[int] = None):
        self.futures_list = []
        self.pbar = log.progress_bar(desc="Downloading files", total=total)
        self.executor = concurrent.futures.ThreadPoolExecutor()

    def download(self, url: str, destination: Path):
        self.futures_list.append(
            self.executor.submit(
                DownloadManager.__download_url_to_file, self, url, destination
            )
        )
        self.pbar.refresh()

    def __download_url_to_file(self, url: str, destination: Path):
        response = requests.get(url, stream=True)
        with open(destination, "wb") as out_file:
            shutil.copyfileobj(response.raw, out_file)
        self.pbar.update()

    def close(self):
        concurrent.futures.wait(self.futures_list)
        self.executor.shutdown()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        # False means if an exception occurred do not suppress
        return False


class VQFilesManager:
    def __init__(self, api_settings: vq.api.ApiSettings, organisation_uuid: UUID):
        self.api_settings = api_settings
        self.organisation_uuid = organisation_uuid

    def download_files(
        self,
        files: List[UUID],
        download_path: Path,
        shutdown_handler: GracefulShutdownHandler,
    ):
        downloaded_files: List[Path] = []
        with DownloadManager(total=len(files)) as download_manager:
            for file_uuid in files:
                if shutdown_handler.interrupted:
                    return

                vq_files_url = (
                    f"{self.api_settings.url}/api/v1/fileReferences/{file_uuid}"
                    f"&organisation={self.organisation_uuid}"
                )
                response = requests.get(
                    url=vq_files_url, headers=self.api_settings.headers
                )

                if response.status_code == 404:
                    raise FileNotFoundError(
                        f"The file uuid {file_uuid} cannot be found"
                    )
                response.raise_for_status()
                file = response.json()

                file_info = file["file"]
                file_url = (
                    file_info["baseUrl"]
                    + "/"
                    + file_info["folder"]
                    + "/"
                    + file_info["fileHash"]
                    + "."
                    + file_info["extension"]
                )
                destination: Path = download_path / file["name"]
                download_manager.download(file_url, destination)

                downloaded_files.append(destination)

        return downloaded_files

    def _create_get_subfolders(self, job: Job, root_folder: Path, files: List[Path]):
        subfolders: Set[Path] = {Path(".")}

        folder_tree = {}
        destination_folder_uuid = str(job.destination_folder_uuid)

        for file in files:
            rel_path = file.relative_to(root_folder)
            ptr = folder_tree

            subfolders.update(rel_path.parents)

            for part in rel_path.parts[:-1]:
                if part not in ptr:
                    ptr[part] = {}
                ptr = ptr[part]

        def tree_to_list(d: Dict[str, Any]) -> List[Dict[str, Any]]:
            return [{"name": k, "children": tree_to_list(v)} for k, v in d.items()]

        subfolders_to_create = tree_to_list(folder_tree)

        if len(subfolders_to_create) == 0:
            return {Path("."): destination_folder_uuid}

        log.log(f"creating/getting subfolders {subfolders}")

        folders_url = (
            f"{self.api_settings.url}/api/v1/fileReferences/folder"
            f"?organisation={self.organisation_uuid}"
        )

        data = {
            "parentFolderUuid": destination_folder_uuid,
            "folders": subfolders_to_create,
        }
        response = requests.post(
            url=folders_url, headers=self.api_settings.headers, json=data
        )

        response.raise_for_status()

        new_folders = response.json()

        subfolder_uuids: Dict[Path, str] = {}

        sorted_subfolders = sorted([p for p in subfolders], key=lambda p: len(p.parts))

        for subfolder in sorted_subfolders:
            if subfolder == Path("."):
                subfolder_uuids[subfolder] = destination_folder_uuid
            else:
                parent_uuid = subfolder_uuids[subfolder.parent]
                matching_folders = [
                    file
                    for file in new_folders
                    if file["folderUuid"] == parent_uuid
                    and file["name"] == subfolder.name
                ]

                assert len(matching_folders) == 1

                subfolder_uuids[subfolder] = matching_folders[0]["uuid"]

        return subfolder_uuids

    def upload_files(self, folder_uuid: UUID, files: List[Path]):
        upload_url = (
            f"{self.api_settings.url}/api/v1/fileReferences/file"
            "?overwrite=overwrite"
            "&runTriggers=true"
            f"&organisation={self.organisation_uuid}"
        )

        for file_path in log.progress_bar(files, desc="Uploading"):
            with open(file_path, "rb") as file_data:
                files_body = {
                    "files_in": (file_path.name, file_data),
                }
                data_body = {
                    "folder_uuid": folder_uuid,
                }

                log.log(f"Uploading {file_path.name}")

                response = requests.post(
                    url=upload_url,
                    headers=self.api_settings.headers,
                    files=files_body,
                    data=data_body,
                )
                response.raise_for_status()
