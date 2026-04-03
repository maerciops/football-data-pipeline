# Football Data Pipeline — Brasileirão

**A Product-Driven Data Engineering Project.**

Instead of building a "cemetery of perfectly orchestrated parquets" just to showcase modern data tools, this pipeline was reverse-engineered from the final product. The goal is to answer the real questions a football fan asks on a Monday morning after a league match.

The entire data architecture (Docker, Airflow, dbt, Spark) exists solely to extract, process, and deliver reliable data to answer three core business questions:

1. **Luck or Merit?** (Comparing Actual Goals vs. Expected Goals - xG).
2. **Is a single player carrying the team?** (Analyzing individual impact metrics).
3. **Is our squad actually superior?** (Measuring the impact of the bench/substitutes against rivals).

---

## Architecture and Stack

The project is designed with a software engineering mindset, scaling from a local script to a modern, containerized data infrastructure:

* **Extraction:** Python with Selenium & BeautifulSoup (Cloudflare/Anti-bot bypass).
* **Containerization:** Docker & Docker Compose (Isolated environments, custom images with Virtual Framebuffer for headless scraping).
* **Storage:** Columnar **Apache Parquet** files (Schema preservation and performance).
* **Orchestration:** Apache Airflow *(Work in Progress)*.
* **Data Modeling:** dbt & Apache Spark *(Upcoming)*.
* **Dashboard:** Streamlit *(Upcoming)*.

---

## How to Run (Docker-First Approach)

This project uses Docker to ensure isolated and reproducible environments. You do not need to install Chrome, ChromeDriver, or Python locally. 

### Prerequisites
* [Docker](https://docs.docker.com/get-docker/)
* [Docker Compose](https://docs.docker.com/compose/install/)

### Execution

To build the custom image and trigger the data extraction process:

```bash
# Clone the repository
git clone [https://github.com/your-username/football-data-pipeline.git](https://github.com/your-username/football-data-pipeline.git)
cd football-data-pipeline

# Build and run the extractor container
docker-compose --profile extractor up --build
```
Note: The docker-compose.yml is explicitly configured with shm_size: 2gb to handle the heavy RAM usage of the headless Chromium browser when rendering complex statistical tables.

---

## How the Extractor Works (fbref.py)
1. The extraction module was built with resilience and data integrity in mind:

2. Human Simulation: Implements User-Agent rotation and randomized delays to avoid server bans.

3. Hidden Data Scraping: Parses data hidden inside HTML comments (a known FBref strategy to protect advanced metrics).

4. Transformation: Converts raw HTML into clean Pandas DataFrames.

5. Persistence: Saves the output locally in the data/ directory as .parquet files via Docker bind mounts.

## Project Structure

/football-data-pipeline
├── /dags                 # Airflow Orchestration (WIP)
├── /data                 # Parquet file storage via volume bind mounts
├── /dbt_models           # dbt data modeling
│   ├── /marts
│   └── /staging
├── /logs                 # Execution logs
├── /spark_jobs           # Apache Spark processing scripts
├── /src
│   ├── /collectors       # Extraction logic
│   │   └── fbref.py      # Main scraper (Selenium + BS4)
│   └── /utils            # Shared utilities
├── /streamlit_app        # Dashboard application
├── Dockerfile            # Custom image (Python + Chromium + Xvfb)
├── docker-compose.yml    # Services orchestration
├── requirements.txt      # Python dependencies
└── .gitignore            # Ignores local data, logs, and binaries

---

## Roadmap & Progress

• [x] Phase 1: Robust data extraction via Selenium & Pandas.

• [x] Phase 2: Environment containerization (Docker, Xvfb, memory management).

• [ ] Phase 3: Automated orchestration via Apache Airflow & DockerOperator.

• [ ] Phase 4: Cloud integration (AWS S3) and data processing (Apache Spark).

• [ ] Phase 5: Data Warehouse modeling (dbt) and Visualization (Streamlit).

### Author
Maercio Paulino de Sousa Software Engineer | Data Engineering [https://www.linkedin.com/in/maercio-paulino/]
