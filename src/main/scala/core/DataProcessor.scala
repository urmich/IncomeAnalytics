package core

import org.apache.spark.sql.DataFrame

/**
  * Base for all data processing classes
  * It implements Template pattern
  */
trait DataProcessor extends SparkAware {
	
	/**
	  * Abstract method for orchestration of reading data
	  * @return DataFrame with read data
	  */
	def read(): DataFrame
	
	/**
	  * Abstract method for orchestration of transforming data
	  * @param data - DataFrame with data to be transformed
	  * @return - DataFrame with data after transformation
	  */
	def transform(data: DataFrame): DataFrame
	
	/**
	  * Abstract method for orchestration of writing data
	  * @param data - DataFrame with data to be written
	  */
	def write(data: DataFrame): Unit
	
	/**
	  * Template method that implements the flow of data from the source to target.
	  *This method cannot be overriden to prevent changing the template
	  */
	final def process(): Unit = {
		
		val rawData: DataFrame = read()
		
		val transformedData: DataFrame = transform(rawData)
		
		write(transformedData)
		
		spark.stop()
	}
}
