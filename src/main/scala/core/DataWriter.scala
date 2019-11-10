package core

import org.apache.spark.sql.DataFrame

/**
  * Abstract class for data writer
  */
trait DataWriter extends SparkAware {
	
	/**
	  * Abstract method for writing data
	  * @param data - DataFrame with data to be written
	  */
	def write(data: DataFrame): Unit
}
