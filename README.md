<a id="readme-top"></a>

<br />
<div align="center">

  <h3 align="center">DataExtraction</h3>

  <p align="center">
    Automated ETL pipelines for extracting data from MySQL databases and REST APIs using Python.
    <br />
    <a href="https://github.com/Dhaanesh26/DataExtraction"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/Dhaanesh26/DataExtraction">View Demo</a>
    ·
    <a href="https://github.com/Dhaanesh26/DataExtraction/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    ·
    <a href="https://github.com/Dhaanesh26/DataExtraction/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>

</div>


<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a></li>
    <li><a href="#architecture">Architecture</a></li>
    <li><a href="#built-with">Built With</a></li>
    <li><a href="#getting-started">Getting Started</a></li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#incremental-load-support">Incremental Load Support</a></li>
    <li><a href="#docker-setup">Docker Setup</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>


<!-- ABOUT THE PROJECT -->
## About The Project

This project provides scalable data extraction from:

1. MySQL Database  
2. REST API Endpoints

Extracted data is validated and saved in the `output/` directory as CSV/JSON.

### 🔑 Highlights
- Modular ETL pipelines
- Error-handled DB/API connectivity
- Config-driven architecture
- Supports batching and pagination
- Logging enabled

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- ARCHITECTURE -->
## Architecture

    ┌────────────┐        ┌───────────┐
    │  MySQL DB  │        │ REST API  │
    └─────┬──────┘        └─────┬─────┘
          │                       │
          ▼                       ▼
    ┌──────────────────────────────────┐
    │         ETL Pipeline             │
    │    (Python + Config Driven)      │
    └─────────────────┬────────────────┘
                      ▼
              ┌──────────────┐
              │ Output (.csv)│
              │ Output (.json)│
              └──────────────┘



<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- BUILT WITH -->
## Built With

* Python 3.9+
* Pandas
* MySQL Connector
* Requests API Library
* PyYAML

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- GETTING STARTED -->
## Getting Started

### Prerequisites

-----------------------------------------------------------------
pip install -r requirements.txt

Clone project

git clone https://github.com/Dhaanesh26/DataExtraction.git
cd DataExtraction

Update configurations

config/database_config.yaml
config/api_config.yaml

Run MySQL extraction:

python3 src/mysql_extractor.py

Run API extraction:

python3 src/api_extractor.py

Usage

output/mysql/
output/api/

Logs are saved under:

logs/data_extraction.log

Configurable via:

config/incremental_config.yaml

Docker setup:

docker-compose up -d
-----------------------------------------------------------------

* Roadmap

- MySQL Extraction

- REST API Extraction

- Docker MySQL Support

- Incremental Data Warehouse Load

- Airflow Orchestration

- Unit Testing + CI/CD Integration

Contact

Dhaanesh
📩 Email: (add your email here)
🔗 LinkedIn: https://linkedin.com/in/dhaanesh-s

Repo Link: https://github.com/Dhaanesh26/DataExtraction

Acknowledgments

Python Docs

MySQL Docs

Requests Library Docs

Shields.io


