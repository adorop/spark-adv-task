#Spark Task

## Description

Reads _Tweet_ entities from Kafka, groups them by hash-tags and country and calculates count, writes the result into HDFS 
partitioned by day and hour.


When a Kafka data has message(s) which intersect  with partitions already stored in HDFS, entire partition is updated 

## Modules

#### Spec

Contains abstract specs for Unit and Integration tests

#### Hdfs-incremental-update

Implements logic of writing new data to HDFS, handling partitions' updates.

Spark fails to overwrite existing partitions. The way it works is:
1. set _DataFrameWriter.mode("Overwrite")_
1. Spark analyses plan, i.e. resolves input paths (which is the same as output in this case)
1. Spark removes output folder before reading
1. executors encounter _FileNotFoundException_

That is why partitions with a new data are written to a _delta_ directory as a result of Spark Job and used to replace 
previously created partitions by using Java HDFS API


#### Tweets-count-by-hashtags-and-counrty

Implements logic of calculating count by hashtags and country. 

Country is represented as _country code_, therefore doesn't
require _normalization_, whereas _hashtags_ are provided by users that's why they're lowercased and, since a _Tweet_ may 
have multiple _hashtags_, [explode](https://spark.apache.org/docs/1.6.1/api/java/org/apache/spark/sql/functions.html#explode(org.apache.spark.sql.Column)) function is applied.

## Build
[pom.xml](pom.xml) requires neither profiles nor system properties to be provided 

```
$ mvn clean package
```

## Run

Requires path to a _json_ configuration file to be passed as a command line argument. Example configuration:

```
{
  "kafkaReaderConfiguration": {
    "bootstrapServers": [
      "localhost:6667"
    ],
    "topic": "tweets_topic",
    "partitions": [
      {
        "partition": "0",
        "startIndex": -2,
        "endIndex": -1
      }
    ]
  },
  "hdfsTableConfiguration": {
    "readOptions": {},
    "format": "json",
    "path": "/path/to/output/directory",
    "writeOptions": {}
  }
}

```

_SparkSession_ is not configured in the Scala code, thus, mandatory properties should be provided at _submit_ time. 
```
$ spark-submit --master yarn  \
>  tweets-count-by-hashtags-and-country-1.0.jar config.json 
```