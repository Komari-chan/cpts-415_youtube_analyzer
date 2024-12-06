# YouTube Analyzer

## Overview

The **YouTube Analyzer** is a tool designed to process, analyze, and visualize YouTube video datasets. 
It utilizes **MongoDB**, **PySpark**, and **PyQt5 GUI** to provide insights into video trends, categories, and user behaviors. 
This tool can be useful for content creators, marketers, and social media analysts seeking data-driven strategies.

## Features

- **Data Extraction**: Fetch YouTube video data from a MongoDB database.
- **Data Cleansing**: Handle missing or invalid values for seamless analysis.
- **Advanced Filtering**: Filter datasets by age, views, and ratings dynamically.
- **Top-N Analysis**: Identify top videos based on views, ratings, and comments.
- **Visualizations**: Generate insightful graphs, including:
  - Views distribution
  - Category-based statistics
  - Time-based trends for views and ratings
- **Scalability**: Process large datasets using PySpark for distributed computation.
- **Interactive GUI**: Explore data and results via a user-friendly interface.

## Project Structure
```plaintext
├── backend/
│   ├── data_analysis.py        # Functions for data extraction and analysis
│   ├── mongo_connection.py     # MongoDB connection handler
│   ├── filter_data.py          # Data filtering logic
│   ├── visualization.py        # Graph generation functions
├── frontend/
│   ├── qt_frontend.py          # PyQt5 GUI implementation
├── main.py                     # Entry point for CLI usage
└── output/                     # Directory for generated outputs (CSV files, images)
```

## Future Work

Throughout the development of this application there were several issues that need to be resolved (ideally in sequence).
1. Modify ParseAndLoad.py to account for duplicates (possibly adding unique indeces for easier filtering as well).
2. Add ingestion performance for loading the data into MongoDB.
3. Shift functionality from Python Pandas to Pre-Processing MongoDB methods.
4. Fix integrationg with Spark to utilize the visualizations and analyzation for improved scalability.
5. Finish implementing PageRank.
6. Considerable UI changes.

