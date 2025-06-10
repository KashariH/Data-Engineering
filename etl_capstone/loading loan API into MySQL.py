import requests # Importing requests to get data from a web API 
from pyspark.sql import SparkSession # Importing SparkSession to create a Spark session

# Create Spark session
spark = SparkSession.builder \
    .appName("Loan Application ETL") \
    .getOrCreate() 


# Step 1: Read API data
response = requests.get("https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json")  # Fetches data from the specified URL (API endpoint) and stores it in response variable.

# Step 2: Print status code
print("Status Code:", response.status_code) # Displays the status code of the API response ( # 200 = success)

# Step 3: If OK, convert JSON to PySpark DataFrame and load to MySQL
if response.status_code == 200:  # Checks if the API request was successful (code 200)
    data = response.json()  # Converts the JSON response (text format) into a Python dictionary or list so it can be processed.

    # Step 4: Use existing Spark session (spark is already defined)
    # Step 5: Create Spark DataFrame from JSON data
    dataframe = spark.read.json(spark.sparkContext.parallelize(data)) # Converts the JSON data into a Spark DataFrame, which is a distributed collection of data organized into named columns.

    mysql_url = "jdbc:mysql://localhost:3306/creditcard_capstone"  # MySQL database URL (host, port, and database name)

    mysql_properties = {
        "user": "root",               # MySQL username
        "password": "password",       # MySQL password
        "driver": "com.mysql.cj.jdbc.Driver"  # MySQL JDBC driver class name
    }

    # Step 6: Write to MySQL
    dataframe.write \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", "CDW_SAPP_loan_application") \
        .option("user", mysql_properties["user"]) \
        .option("password", mysql_properties["password"]) \
        .option("driver", mysql_properties["driver"]) \
        .mode("overwrite") \
        .save()  # actually writes the data.

    print("Data loaded to MySQL!") # Indicates that the data has been successfully loaded into the MySQL database.
else:
    print("Failed to fetch data.") 