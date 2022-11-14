import os
import json
import requests


class GitLab():
    def __init__(self, access_token, user_id):
        self.base_url = 'https://gitlab.com/api/v4'
        self.access_token = access_token
        self.user_id = user_id
        self.headers = {
                'Authorization': f'Bearer {self.access_token}'
            }


    def get_projects(self,):
        url = f"{self.base_url}/users/{self.user_id}/projects"
        response = requests.request("GET", url, headers=self.headers)
        data = response.json()
        path = os.path.join(os.getcwd(), 'gitlab/repositories')
        if not os.path.exists(path):
            os.makedirs(path)
        file_path = os.path.join(
            path, f'{self.user_id}-repositories.json')
        with open(file_path, "w") as outfile:
            json.dump(data, outfile)
        return data


    def get_merge_requests(self):
        projects = self.get_projects()
        for project in projects:
            url = f"{self.base_url}/projects/{project['id']}/merge_requests"
            response = requests.request("GET", url, headers=self.headers)
            data = response.json()
            path = os.path.join(os.getcwd(), 'gitlab/pull-requests')
            if not os.path.exists(path):
                os.makedirs(path)
            file_path = os.path.join(
                path, f"{self.user_id}-{project['id']}-pull-requests.json")
            with open(file_path, "w") as outfile:
                json.dump(data, outfile)
