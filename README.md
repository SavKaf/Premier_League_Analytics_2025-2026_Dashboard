# Premier League Analytics 2025/26
Premier League Analytics Dashboard for season 2025/26

### Project Overview

This project implements an end-to-end football analytics pipeline using Databricks and Power BI. Match and team data are automatically collected from the Football API and processed in Databricks Community Edition using Python. The ETL pipeline extracts relevant information from the API responses, cleans the data, and transforms it into structured analytical tables.

The transformed data is modeled into fact and dimension tables to support analytical queries and performance metrics. These tables are then connected to Power BI, where relationships are defined using primary and foreign keys. Additional DAX measures are implemented to calculate advanced metrics such as Form Index, Momentum, Attack Rating, Defence Rating, and Points per Match. The final result is an interactive dashboard that allows users to explore team performance, recent match results, and attacking or defensive strengths across the league.

## Key Features

- Automated data ingestion from Football API (https://www.football-data.org/)
- ETL pipeline built in Databricks CE
- Analytical data model for football performance metrics
- Interactive Power BI dashboard

## ## Key Metrics

The dashboard includes several custom performance indicators to evaluate team performance:

- Form Index**: A calculated metric that reflects a team's recent performance based on match results.
- Momentum Indicator**: Shows whether a team is improving, declining, or maintaining stable performance.
- Last 5 Matches**: Visual representation of recent results to quickly identify performance trends.
- Attack Rating**: A metric that measures offensive strength based on goals scored and attacking statistics.
- Defence Rating**: Indicates defensive performance based on goals conceded and defensive statistics.
- Points per Match**: Average number of points earned per match throughout the season.


## Dashboard Preview

<img width="1394" height="796" alt="Dashboard - Premier League" src="https://github.com/user-attachments/assets/d625e0d5-f55e-4e60-bae3-00da6083196b" />


## Technologies Used

- Databricks Community Edition

- Python

- Football API

- Power BI

- Figma (Dashboard Design)
