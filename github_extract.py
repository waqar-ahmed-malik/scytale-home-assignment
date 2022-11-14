import os
import json
import requests


class GitHub():

    def __init__(self, access_token):
        self.access_token = access_token
        self.headers = {
                'Authorization': f'Bearer {self.access_token}'
            }
        self.base_url = 'https://api.github.com'
    
    def get_repositories(self,):
        url = f"{self.base_url}/user/repos"
        response = requests.request("GET", url, headers=self.headers)
        data = response.json()
        path = os.path.join(os.getcwd(), 'github/repositories')
        if not os.path.exists(path):
            os.makedirs(path)
        file_path = os.path.join(
            path, '{}-repositories.json'.format(data[0]['owner']['id']))
        with open(file_path, "w") as outfile:
            json.dump(data, outfile)
        return data


    def get_pull_requests(self,):
        repositories = self.get_repositories()
        for repository in repositories:
            url = f"{self.base_url}/repos/{repository['full_name']}/pulls"
            params = {"state": "all"}
            response = requests.request(
                "GET", url, headers=self.headers, params=params)
            data = response.json()
            path = os.path.join(os.getcwd(), 'github/pull-requests')
            if not os.path.exists(path):
                os.makedirs(path)
            file_path = os.path.join(
                path, f"{repository['owner']['id']}-{repository['id']}-pull-requests.json")
            with open(file_path, "w") as outfile:
                json.dump(data, outfile)
