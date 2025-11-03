<!-- Improved compatibility of back to top link -->
<a id="readme-top"></a>

<!-- PROJECT SHIELDS -->
[![Python][python-shield]][python-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]
[![Last Commit][last-commit-shield]][last-commit-url]
[![Workflow][workflow-shield]][workflow-url]

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
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>


<!-- ABOUT THE PROJECT -->
## About The Project

This project provides scalable data extraction from:

✅ MySQL Database  
✅ REST API Endpoints

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
```sh
pip install -r requirements.txt


git clone https://github.com/Dhaanesh26/DataExtraction.git
cd DataExtraction

config/database_config.yaml
config/api_config.yaml

python3 src/mysql_extractor.py

python3 src/api_extractor.py

output/mysql/
output/api/

<p align="right">(<a href="#readme-top">back to top</a>)</p> <!-- INCREMENTAL LOAD SUPPORT -->

