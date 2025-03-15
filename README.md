# Data Engineering Task: AdTech Data Pipeline

## Overview

You are tasked with developing a data pipeline for an advertising platform. 

The source data is stored in PostgreSQL (operational database) and needs to be transformed and loaded into ClickHouse (analytical database) for efficient reporting and KPI analysis.

## Task Requirements

Your challenge is to:

1. **Design and implement a ClickHouse schema** optimized for analytical queries
2. **Create a data pipeline** to move data from PostgreSQL to ClickHouse
   - You can use any approach.
   - Your solution should be reproducible and well-documented
3. **Develop queries** to calculate key advertising KPIs
4. **Document your approach** and any assumptions made

## Prerequisites

* [uv](https://docs.astral.sh/uv/getting-started/installation/)
* [docker](https://docs.docker.com/engine/install/)
* [compose](https://docs.docker.com/compose/install/)

## Setup & Environment

This repository provides a complete environment to get started:

```bash
# Install dependencies
uv sync

# Start all services
docker-compose up -d

# Wait a bit for services to initialize, then seed data
uv run python main.py batch
```

## Data Model

The source PostgreSQL database has the following schema:

- **advertiser**: Information about companies running ad campaigns
- **campaign**: Ad campaigns configured with bid amounts and budgets  
- **impressions**: Records of ads being displayed
- **clicks**: Records of users clicking on ads

Detailed schema information can be found in `migrations/V1__create_schema.sql`.

## Data Generation

A data generator is provided to populate the source PostgreSQL database:

```bash
# Generate a complete batch of test data
uv run python main.py batch --advertisers 5 --campaigns 3 --impressions 1000 --ctr 0.08
# Add a single advertiser
uv run python main.py advertisers --count 1
# Add campaigns for an advertiser
uv run python main.py campaigns --advertiser-id 1 --count 2
# Add impressions for a campaign
uv run python main.py impressions --campaign-id 1 --count 500
# Add clicks for a campaign (based on existing impressions)
uv run python main.py clicks --campaign-id 1 --ratio 0.12
# View current data statistics
uv run python main.py stats
# Reset all data (use with caution)
uv run python main.py reset
```

## Deliverables

Please provide the following:

1. **ClickHouse Schema**: SQL scripts to create your analytical tables
2. **Data Pipeline**: Code and configuration to move data from PostgreSQL to ClickHouse
3. **KPI Queries**: SQL queries to calculate the following metrics:
   - Click-Through Rate (CTR) by campaign
   - Daily impressions and clicks
   - Anything else you might find itneresting
4. **Documentation**: A README explaining your design decisions and how to run your solution

## Evaluation Criteria

Your solution will be evaluated based on:

- **Data modeling**: Appropriate schema design for analytical queries in ClickHouse
- **Pipeline architecture**: Choice of tools, approach to data synchronization, and handling of updates
- **Implementation quality**: Reliability, error handling, monitoring, and efficiency
- **Query performance**: Efficient and accurate KPI calculations in ClickHouse
- **Documentation**: Clear explanation of your approach, design decisions, and trade-offs
- **Innovation**: Creative solutions to the data engineering challenges presented

## Technical Requirements

- Your solution should be containerized or have clear setup instructions
- If using Python code, use Python 3.12+ and include dependency information
- The pipeline should be able to handle both initial loads and incremental updates
- All ClickHouse SQL must be compatible with the latest ClickHouse syntax
- Your approach should consider performance, maintainability, and error handling

## Getting Started

1. Clone this repository
2. Install dependencies: `uv sync`
3. Start Docker containers: `docker-compose up -d`
4. Populate test data: `uv run python main.py batch`
5. Explore the sample data:
   - Command line: `uv run python main.py stats`
   - Web interfaces: pgAdmin and Tabix (see Database Access section)
6. Design your ClickHouse schema
7. Implement your ETL pipeline
8. Develop and test your KPI queries
9. Document your solution

For local development:
- View container status: `docker-compose ps`
- View logs: `docker-compose logs`
- Reset data: `uv run python main.py reset`
- Stop services: `docker-compose down`

Good luck!