#!/usr/bin/python3
# coding=utf-8
# pylint: disable=E1101

""" RPC """

import os
import zipfile
from pathlib import Path
from io import BytesIO

from pylon.core.tools import web, log  # pylint: disable=E0401
import requests  # pylint: disable=E0401


class RPC:  # pylint: disable=R0903
    """ RPC pseudo-class """

    @web.rpc('get_build_meta', 'get_build_meta')
    def get_build_meta(self):
        """ Get build meta """
        return self.build_meta

    @web.rpc('update_build_meta', 'update_build_meta')
    def update_build_meta(self, build_meta):
        """ Update build meta """
        self.build_meta.update(build_meta)

    @web.rpc('update_ui', 'update_ui')
    def update_ui(self, release=None):
        """ Download and install UI react app code """
        try:
            from tools import this  # pylint: disable=E0401,C0415
            this.for_module("bootstrap").module.get_bundle(
                "EliteAUI.zip",
                processing="zip_extract",
                extract_target=Path(self.bp.static_folder).joinpath(self.elitea_base_path),
                extract_cleanup=True,
                extract_cleanup_skip_files=[".gitkeep"],
            )
            log.info("Updated UI using bundle")
        except:  # pylint: disable=W0702
            self.update_ui_from_github(release=release)

    @web.rpc('update_ui_from_github', 'update_ui_from_github')
    def update_ui_from_github(self, release=None):
        """ Download and install UI react app code """
        # Compute URL and path
        if release is None:
            release = self.default_release
        #
        log.info("Updating UI for release: %s", release)
        #
        destination_path = Path(self.bp.static_folder).joinpath(self.elitea_base_path)
        # Download, clean-up, extract
        headers = {
            "X-GitHub-Api-Version": "2022-11-28",
            "Accept": "application/json",
        }
        if self.auth_token is not None:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        #
        response = requests.get(
            f"https://api.github.com/repos/{self.release_owner}/{self.release_repo}/releases",
            headers=headers,
            verify=self.release_verify,
        )
        response.raise_for_status()
        releases = response.json()
        #
        for release_info in releases:
            if release_info["name"] == release:
                headers["Accept"] = "application/octet-stream"
                #
                response = requests.get(
                    release_info["assets"][0]["url"],
                    headers=headers,
                    verify=self.release_verify,
                )
                #
                if response.ok:
                    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                        # Clean-up here: at least ZIP was downloaded and opened OK
                        for root, dirs, files in os.walk(destination_path, topdown=False):
                            for name in files:
                                if name in [".gitkeep"]:
                                    continue
                                try:
                                    os.remove(os.path.join(root, name))
                                except:  # pylint: disable=W0702
                                    log.exception("Failed to remove file: %s, skipping", name)
                            for name in dirs:
                                try:
                                    os.rmdir(os.path.join(root, name))
                                except:  # pylint: disable=W0702
                                    log.exception("Failed to remove dir: %s, skipping", name)
                        # Extract new files
                        zip_file.extractall(destination_path)
                    #
                    log.info(f"Zip file downloaded and extracted to {destination_path}")
                else:
                    log.error(f"Failed to download the zip file. Status code: {response.status_code}")
                #
                break
