# Workshop 002
The following project aims to make use of datasets containing information about the Grammy Awards in their different editions and about Spotify songs. The objective is to find a relationship between these two datasets, all of this through the use of ETL (extract, transform, and load) tools and the Apache Airflow workflow management platform.
## Tools and resources
- [x] Python: As the main programming language to perform various activities.
- [x] Virtual machine: To make use of controlled environments and work with Apache Airflow.
- [x] Airflow: To manage the workflow of defined tasks (Data Pipeline).
- [x] MySQL: As the database management system, where initial and final data will be stored.

Two datasets from Kaggle will be used, one from Spotify and the other about the Grammy Awards. Below are the links to download them.
- [Dataset Spotify](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset)
- [Dataset Grammy Awards](https://www.kaggle.com/datasets/unanimad/grammy-awards)

## How to replicate this work?
1. Virtual Machine: Set up your virtual machine to work from it.
2. Python: Verify that Python is installed using `python --version` or `python3 --version`. If it is not installed, run `apt install python3-pip`.
3. Repository: Clone this repository using `git clone [url]` and navigate to the added folder.
4. Environment: Execute `python -m venv venv` to create a new environment or virtual environment.
   - Then execute `source venv/bin/activate` to activate the environment.
