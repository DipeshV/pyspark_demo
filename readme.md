
#### PySpark Demo https://punchh.com 

#### Installation
`$ make install`

#### Linting and testing
```
$ make lint
$ make test
```
#### Set up Databricks dev env at local mac
```
* Reference: https://docs.databricks.com/dev-tools/databricks-connect.html
    * Your Spark job is planned in local but executed on the remote cluster
    * Allow the user to step through and debug Spark code in the local environment

* requirements.txt file example:
databricks-connect(=6.3.0)
* Configuration
    * The trick is one cannot mess up the delicate databricks-connect and pyspark versions

* Test with this example:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
print(spark.range(100).count())  # it executes on the cluster, where you can see the record

```
