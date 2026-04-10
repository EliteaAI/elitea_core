import logging
import os
import zipfile

import requests


def download_github_repo_zip(
        repo_owner: str, repo_name: str, local_dir: str = '.', branch: str = 'main'
) -> dict:
    zip_url = f"https://github.com/{repo_owner}/{repo_name}/archive/refs/heads/{branch}.zip"

    try:
        response = requests.get(zip_url)
        response.raise_for_status()

        local_zip_path = f"{local_dir}/{repo_name}.zip"
        with open(local_zip_path, 'wb') as file:
            file.write(response.content)
        logging.info(f"Repository '{repo_name}' downloaded successfully and saved to '{local_zip_path}'")

        return {'ok': True, 'path': local_zip_path}
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        return {'ok': False, 'error': str(http_err)}
    except requests.exceptions.RequestException as err:
        logging.error(f"Error occurred during request: {err}")
        return {'ok': False, 'error': str(err)}
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return {'ok': False, 'error': str(e)}


def unzip_file(zip_path: str, extract_to: str, subfolder: str = None) -> dict:
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            if not subfolder:
                zip_ref.extractall(extract_to)
                logging.info(f"File '{zip_path}' extracted successfully to '{extract_to}'")
            else:
                all_files = zip_ref.namelist()
                # filter files to include only those in the specified subfolder
                subfolder_files = [f for f in all_files if f.startswith(subfolder)]
                for file in subfolder_files:
                    zip_ref.extract(file, extract_to)
                logging.info(f"Subfolder '{subfolder}' from file '{zip_path}' extracted successfully to '{extract_to}'")
            os.remove(zip_path)
            logging.info(f"File '{zip_path}' was removed successfully")
            return {'ok': True}
    except zipfile.BadZipFile as bzf:
        logging.error(f"Error: The file '{zip_path}' is not a zip file or it is corrupted.")
        return {'ok': False, 'error': str(bzf)}
    except Exception as e:
        logging.error(f"An error occurred while extracting the file: {e}")
        return {'ok': False, 'error': str(e)}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    #
    static_dir: str = '../data'
    zip_path = download_github_repo_zip(
        repo_owner='EliteaAI', repo_name='elitea_static', local_dir=static_dir
    )
    #
    unzip_file(zip_path.get('path'), static_dir, subfolder='elitea_static-main/tool_icons')
