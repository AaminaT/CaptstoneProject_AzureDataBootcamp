# CaptstoneProject_AzureDataBootcamp

Through ZyberTech's 3-month Data Bootcamp, I have developed key skills in cloud computing and data engineering. I have understood the role of a data engineer and became familiar with the Azure environment. The tools I am familiar with include:
- Databricks
- Data Factory
- PowerBI
- SQL Server
- SQL Databases
- Data Studio
- DevOps
- Data Lake Storage
- Logic Apps
- Virtual Machines
- Resource Group
- 
I have acquired knowledge and applied ETL pipelines, developed data models, and practiced my knowledge of SQL and Python. Additionally, I am confident in discussing cloud concepts and understanding the power of cloud computing. Through my project, I also learned a little Scala for my calendar dimension table.

Note: Please download the .HTML version to view my code from Databricks, along with the data frames. Please feel free to import the notebook for your data analysis. My data model focuses on the order level granularity. For future exploration, an analysis can be done on product and seller information.

## 1. Develop a Project Plan

Once the boot camp started, I developed a project plan on Workday to organize the tasks I needed to complete, along with an estimated time frame. This kept me accountable and helped me meet deadlines. The Olist dataset on Kaggle intrigued me because it was real data from a company in Brazil. Since I am interested in eCommerce data, this is a good challenge. 

![My Logo](https://drive.google.com/uc?export=download&id=1geiESAD_3t0L2Az3KSNllyhfB_gwz1XO "Logo Title Text 1")
![My Logo](https://drive.google.com/uc?export=download&id=1ZLbQPb22g-pDrfbNl1aZxO0CwaPRHg70 "Logo Title Text 3")
![My Logo](https://drive.google.com/uc?export=download&id=1z4xyYk6kA1JCMsHLwjmTAHPSMgmTrhRM "Logo Title Text 2")

## 2. Loading Data
I downloaded the Olist data from Kaggle and pushed my files into my source folder in the Azure Data Lake Storage Account. From there, I created a pipeline in Azure Data Factory to move my data from the source to the raw folder as parquet files. I developed a workflow to transfer the data smoothly. When attempting to convert the reviews.csv into a parquet file, the pipeline failed due to special characters, causing the file to terminate early. As a result, I added a filter in ADF to convert certain columns. The columns with special characters were columns I was not planning to use for my analysis, so removing the data was the best option. 








