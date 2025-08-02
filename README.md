# Disease Outbreak Prediction using Bluesky & Google Trends Data

This project explores whether social media posts (Bluesky) and search trends (Google Trends) can be used to predict flu and related illness outbreaks in real time. Developed as part of a team project, the pipeline combines data streaming, unsupervised learning, and time-series forecasting to surface potential early indicators of public health trends.

## Overview

With the rise of real-time social media and widespread symptom-related searches, we sought to harness these signals to predict flu-related outbreaks. The project incorporates both batch and simulated streaming pipelines, allowing us to visualize evolving patterns and cluster health-related discussions in near real time.

## Team Members

This project was completed by a team of three:

- **Mildred Nwachukwu-Innocent**
- **Bryn Hughes**
- **Camila Torres**

## Technologies Used

- **Bluesky Data** (simulated streaming via Kafka)
- **Google Trends API**
- **Kafka** (producer/consumer pipeline)
- **Scikit-learn** (K-Means, LDA)
- **Facebook Prophet** (time series modeling)
- **Flask** (backend API)
- **Power BI** (real-time data visualization)
- **NLTK / spaCy** (NLP preprocessing)
- **Pandas / NumPy** (data wrangling)

## Pipeline Design

### Offline Pipeline:
- Batch processing of Bluesky data based on hashtags and keywords.
- Preprocessing including NLP, feature engineering, and dimensionality reduction.
- Clustering using K-Means and topic modeling via LDA.
- Forecasting search trends using Prophet on Google Trends data.

### Online Pipeline (Simulated Streaming):
- Simulated real-time ingestion of Bluesky messages via Kafka.
- Filtered using health-related keywords (e.g., "flu", "fever", "cough").
- Processed messages pushed to Flask API and visualized in Power BI.


##  Dashboard

The real-time results were fed into a **Power BI dashboard** using the Flask API backend. This enabled dynamic updates and user-friendly visualization of trends and anomalies.

##  Acknowledgments

Special thanks to Bryn and Camila for their collaboration and dedication to this project.
