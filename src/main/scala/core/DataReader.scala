package core

import org.apache.spark.sql.DataFrame

/**
  * Base abstract class for data reader
  */
trait DataReader extends SparkAware {
	
	/**
	  * Abstract method for reading data
	  * @return DataFrame with read data
	  */
	def read(): DataFrame
}