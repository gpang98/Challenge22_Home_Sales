# Analysis using SparkSQL to determine key metrics about home sale data
##### Challenge22_Home_Sales-challenge - UWA/edX Data Analytics BootCamp.

## Overview

This project aim to analyse the Home Sales data using Spark to create temporary views, partition the data, cache and uncache temporary table, and verify that the table has been unchached.

## Objective

The primary goal of this project is:
- To analyse the Home Sales data using Spark, complete some task and answer some questions.


## Dataset

The CSV file weblink `"https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"`is provided and contain the following fields:

- `id` —Identification columns
- `date` —date property was sold
- `date_built` —date property was built
- `price` —Selling price
- `bedrooms` —No of bedrooms
- `bathrooms` —No of bathrooms
- `sqft_living` —living room in sq ft
- `sqft_lot` —land lot size in sq ft
- `floors` —No of floors
- `waterfront` —Facing waterfront (1) or not (0)
- `view` —no of viewings recorded
   

## Tools and Libraries
- `Google Colab`: Environment used to leverage the computing power.
- `Python`: Used for data preprocessing, initial analysis, and visualization.
- `Pandas`: Utilized for data manipulation and analysis.
- `Jupyter Notebook`: Employed as the development environment.
- `sklearn`: module used to do neural network deep prediction.
- `train_test_split()`: Function to split the original dataset to Train and Test datasets.
- `StandardScaler()` and `transform()`: To scale the training and testing features datasets.
- `tensorflow.keras.models.Sequential()`: Function to access the neural network deep prediction.
- `add()`: To add hidden layers.
- `summary()`: To see the summary of the model architecture built.
- `compile()`: Function to compile the model.
- `fit()`: Function to train the model.
- `evaluate()`: To evaluate the model using the test data.
- `save()`: To save the model to HDFS file.


## Workflow
The following is the workflow employed in performing the necessary queries in Google Colab:

1. Import the necessary dependencies: 
	- import os 
	- `spark_version = 'spark-3.5.0'`
	- `os.environ['SPARK_VERSION']=spark_version`
2. Install Spark and Java: 
	- `!apt-get update` 
	- `!apt-get install openjdk-11-jdk-headless -qq > /dev/null`
	- `!wget -q http://www.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop3.tgz`
	- `!tar xf $SPARK_VERSION-bin-hadoop3.tgz`
	- `!pip install -q findspark`
3. Set Environment Variables: 
	- `os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"` 
	- `os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop3"`
4. Start a SparkSession: 
	- import findspark
	- `findspark.init()`
4. Import packages: 
	- from pyspark.sql import SparkSession
	- import time
	- `spark = SparkSession.builder.appName("SparkSQL").getOrCreate()`  - to create SparkSession    
5. Preprocessing the data.
	- from pyspark import SparkFiles
	- Read in the `https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv` from the url.
	- `spark.sparkContext.addFile(url)`
	- `spark.read.csv()` - to read the file into a dataframe and inspect the 
	- inspect the dataframe
6. Create Temporary view from the Dataframe.
	- `createOrReplaceTempView()`


## Usage

1. **Setup Environment:**
   - Access your Google Drive and utilise the Google Colab to run the Jupyter Notebook file.

2. **Educational Purposes:**
   - Feel free to download the uploaded pages so that you can also explore the dataset and gain insights.

## Key Questions:
#### 1. What is the average price for a four bedroom house sold in each year rounded to two decimal places?
| year | avg_price  |
|------|------------|
| 2010 | 296800.75  |
| 2011 | 302141.9   |
| 2012 | 298233.42  |
| 2013 | 299999.39  |
| 2014 | 299073.89  |
| 2015 | 307908.86  |
| 2016 | 296050.24  |
| 2017 | 296576.69  |


#### 2. What is the average price of a home for each year the home was built that have 3 bedrooms and 3 bathrooms rounded to two decimal places?
| year | avg_price |
|------|-----------|
| 2010 | 292859.62 |
| 2011 | 291117.47 |
| 2012 | 293683.19 |
| 2013 | 295962.27 |
| 2014 | 290852.27 |
| 2015 | 288770.3  |
| 2016 | 290555.07 |
| 2017 | 292676.79 |


#### 3. What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors, and are greater than or equal to 2,000 square feet rounded to two decimal places?
| year | avg_price |
|------|-----------|
| 2010 | 285010.22 |
| 2011 | 276553.81 |
| 2012 | 307539.97 |
| 2013 | 303676.79 |
| 2014 | 298264.72 |
| 2015 | 297609.97 |
| 2016 | 293965.1  |
| 2017 | 280317.58 |


#### 4.  What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than or equal to $350,000? Although this is a small dataset, determine the run time for this query.
| avg_view |
|----------|
|    32.26 |

- --- 0.7197608947753906 seconds ---

## Conclusion:
- Using Spark in Google Colab environment is a powerful way to view and query the data.


## References

1. Inspired by lectures notes and ChatGPT.
