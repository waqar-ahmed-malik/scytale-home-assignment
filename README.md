# scytale-home-assignment

## Requirements
Detailed requirements are given [here](scytale-home-assignment.md)

## Directory Structure
```
.
├── configs
│   ├── source-1-config.json
│   ├── source-2-config.json 
│── source-1
│   │── repositories
│   │   ├── <user-1>-repositories.json
│   │   ├── <user-2>-repositories.json
│   └── pull-requests
│       ├── <user-1>-pull-requests.json
│       ├── <user-2>-pull-requests.json
├── source-2
├── source-1-extract.py
├── source-2-extract.py
├── main.py
└── README.md
```

## Overview

### Extraction
Given the requirements, assuming that we have two sources [GitHub](https://github.com/) and [GitLab](https://about.gitlab.com/). 
We have the API Integration setup as separate custom python modules `github_extraction.py` and `gitlab_extraction.py`. Both the modules authenticate with the respective APIs using Access Tokens via Python's `request` module and extracts:
- List of repositories for the given user
- List of Pull Requests for each repository

Once the raw data is extracted, it's being stored in JSON files under source folder name
This extraction is performed in the `main.py` script before normalization.

### Normalization
Based on the extracted data, both repositories and pull requests raw data is being read by spark using `pyspark` python module for each source. The read JSON data is being transformed as a spark DataFrame and gets through multiple transformations including, field rename, field addition and field calculation based on the source config.
Once both the responses and transformed, a final DataFrame is being created by merging the transformed response and the final normalized data is being written to a new JSOn file `<source-name>-normalized.json`


## Config Structure
```
[
    {
        "source": "repository_response",
        "source_field": "owner.username",
        "warehouse_field": "repo_owner",
        "transformation_expression": null,
        "transformation_description": "Extracting and renaming Owner field for the repository"
    },
]
```
where,
- `source`: API response in which the field is available, eg: repository or pull request
- `source_field`: field name in the raw source data
- `warehouse_field`: field name in target warehouse data
- `transformation_expression`: Expression required for this field's transformation
- `transformation_description`: Brief description of the transformation applied

## Environment Setup

### Python Dependencies
`pip3 install -r requirements.txt`
### Environment Variables
- GITHUB_ACCESS_TOKEN
- GITLAB_ACCESS_TOKEN
- GITLAB_USER_ID

### Run Locally on Mac/Linux
- Clone the Repository to Local: `git clone https://github.com/waqar-ahmed-malik/scytale-home-assignment.git`
- Switch to the Local Repository: `cd scytale-home-assignment/`
- Create a Virtual Environments `python3 -m venv .env`
- Activate the Virtual Environment: `source .env/bin/activate`
- Install Python Dependencies: `pip install -r requirements.txt`
- Set Credentials in Environment Variables:
    - `export GITHUB_ACCESS_TOKEN="<REPLACE_WITH_ACTUAL_GITHUB_ACCESS_TOKEN>"`
    - `export GITLAB_ACCESS_TOKEN="<REPLACE_WITH_ACTUAL_GITLAB_ACCESS_TOKEN>"`
    - `export GITLAB_USER_ID="<REPLACE_WITH_ACTUAL_GITLAB_USER_ID>"`
- Run the main script: `python3 main.py`