This project implements 2 Spark Jobs:

1. Job that reads CSV files from the DataFiles directory and saves the data in Hive table in ORC format.
<br/>
2. Job that performs statistical analysis of the stored data:
<br/>
    •	Output the average number of kids (the third column in the csv)
<br/>
    •	Output the total sum of incomes (the fourth column)

Both Spark Jobs use Spark v2.4.1 and are written in Scala 2.11.

Both Spark jobs use configuration as defined in the _app.properties_ file in the _resources_ directory.

The Jobs were ran in the local mode in the IDE.

