# octopus
# The goal is to simplify data processing and processing through SQL, while ensuring the flexibility of Python and providing a higher degree of extensibility, so that all components through Django connection, reduce the use of components barriers, so that all operations more like a relational data writing SQL statements.
# The major components
Django + Spark + hive + hdfs + parquet + ES
# The main function
1. Extract data from different databases and put them into hive, with parquet as the hive file storage format.
2. Pre-generate multi-dimensional multi-level summary CUBE according to dimension attributes, and import data into Elastic Search is optional.
3. SQL query page and Python command page are provided.
4. The configuration page of data extraction is provided to support database extraction configurations such as Oracle and Teradata.
# The main idea
Django maintains the same SparkSession, avoiding data exchange. Python command page and SQL page are based on the same SparkSession, and DataFrame can be directly used with each other.
Avoid the complexity of Python data processing, data preparation can be completed through SQL, processing cycle, judgment logic can be completed through Python;At the same time, keep Python flexible. You can call Pandas, Scipy and other commonly used data analysis packages.
