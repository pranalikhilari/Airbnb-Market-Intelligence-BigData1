# 🏠 Enhancing Airbnb: Insights from Data Analysis and Image Verification

This project leverages advanced data engineering, machine learning, and visualization techniques to enhance the Airbnb experience for hosts and guests across Canadian cities. The objective is to provide actionable insights for optimizing pricing, improving user satisfaction, and ensuring the authenticity of property listings.

---

## ✨ Features and Highlights

### 1. 📂 Data Collection and Preparation
- **Datasets**:
  - 📋 Airbnb Listings Dataset
  - 📝 User Reviews Dataset
  - 🌍 World Cities Dataset
  - 🖼️ Property Images
- Converted Airbnb data from **CSV to JSON** for better compatibility with PySpark.
- Uploaded and partitioned datasets on **Amazon S3** by city for efficient distributed processing.

---

## 2. 🔍 Data Processing and Analysis
- Used **PySpark** for ETL, feature extraction, and geospatial data processing.
- Integrated world cities and college datasets using **broadcast joins** for spatial analysis.
- Calculated distances between listings and landmarks using the **Haversine formula**.

---

## 3. 💬 Sentiment Analysis
- Cleaned and preprocessed reviews using **NLTK** (stopword removal, lemmatization, etc.).
- Generated polarity scores with **TextBlob** and categorized reviews into positive, neutral, and negative sentiments.
- Conducted **Collocation Analysis** using n-grams to identify common phrases in positive and negative reviews.

---

## 4. 🤖 Machine Learning Models

### 🖼️ Property Image Classification
- Trained a **ResNet50** model with transfer learning to classify listing images.
- Model performance:
  - **Validation Accuracy**: 87%
  - **Testing Accuracy**: 80.2%
- Insights derived using a **Confusion Matrix**.

### 📈 Price Prediction
- Implemented a **Gradient Boosted Tree (GBT) Regressor** in PySpark.
- Integrated features like sentiment scores and key property attributes.
- Performance Metrics:
  - **RMSE**: 117.1
  - **R²**: 0.9

---

## 5. 📊 Feature Importance Analysis
- Used PySpark's **VectorAssembler** and **StandardScaler** to process features.
- Applied **Random Forest Regressor** to identify critical features:
  - Pricing: Bathrooms, bedrooms, and recent positive reviews.
  - Ratings: Review frequency and days since last review.

---

## 6. ⚙️ Workflow Automation
- Orchestrated workflows using **Apache Airflow**, managing:
  - Data transformation.
  - Spatial and sentiment analysis.
  - Machine learning tasks.
- Deployed workflows on **Amazon EMR** for scalability.

---

## 7. 📈 Visualization and Reporting
- Created interactive dashboards with **Amazon QuickSight**, showcasing:
  - Revenue by city.
  - Sentiment trends.
  - Feature importance for pricing and reviews.
 <img width="1163" alt="image" src="https://github.com/user-attachments/assets/c6231c73-a786-4029-a1ec-d489f95e95ba">

---

## ⚠️ Challenges and Solutions
- **CSV Parsing Issues**: Converted CSV to JSON for easier processing in PySpark.
- **Visualization Integration**: Used AWS QuickSight for seamless integration with S3-hosted datasets.

---

## 📌 Insights
- 🏘️ Listings near colleges score higher in reviews but show minimal pricing correlation.
- 🧹 Positive reviews emphasize cleanliness and location; negatives focus on maintenance issues.
- 🏢 "Entire Condo" and "Entire Unit" are the most popular property types.
- 🛁 Features like bathrooms, bedrooms, and recent reviews significantly influence pricing and ratings.

---

## 🛠️ Technologies and Frameworks
- **Data Storage**: Amazon S3
- **Data Processing**: PySpark, EMR
- **Workflow Automation**: Apache Airflow
- **Visualization**: Amazon QuickSight
- **Machine Learning**: ResNet50, Gradient Boosted Trees
- **Sentiment Analysis**: NLTK, TextBlob

---

## 🔗 Links
- [📂 Dataset](https://drive.google.com/drive/folders/1hw1nZwLVfkTwQN4o-I-gxkZmoIyOkuxu)
- [📊 Data Visualization Dashboard](https://drive.google.com/file/d/1hqbqPbT5eJl5FfbzcG-brG2SWzaC6wDN/view?usp=sharing)
- [🎥 Video Demonstration](https://www.youtube.com/watch?v=q2HQEbPlqoE)
