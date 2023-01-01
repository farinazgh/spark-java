# Notes

this high-level domain specific code in Scala or its  equivalent relational query in SQL will generate identical code.

a dataset object Person with field names fname, lname, age, weight access using object notation
```scala
val seniorDS = peopleDS.filter(p=>p.age > 55)
``` 
a dataframe with structure with named columns fname, lname, age, weight access using col name notation
```scala
val seniorDF = peopleDF.where(peopleDF(“age”) > 55)
``` 

equivalent Spark SQL code
```scala
val seniorDF = spark.sql(“SELECT age from person where age > 35”)
``` 
