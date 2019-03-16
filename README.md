# basket-analysis

PoC using SPARK's FP-growth algorithm to generate association rules for Premier Basket items.

# Pre-requisites

- Run BasketAnalysisDataImport.exe to import BasketItem records from Boomerang database ( more here: https://github.com/robertmujica/basket-analysis-data-import )

# Implementation highlights

- Transform the data from tasketItem records ( row-based) , into transactions such that we have all the items bought together in one row

TODO image here

- Convert transactions into JavaRDD ( aka Java Resilient Distributed Dataset, https://spark.apache.org/docs/1.6.0/api/java/org/apache/spark/rdd/RDD.html ) 

# Useful links 

- https://spark.apache.org/docs/2.2.0/ml-frequent-pattern-mining.html

