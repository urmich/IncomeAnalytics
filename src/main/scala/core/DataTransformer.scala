package core

import org.apache.spark.sql.DataFrame

/**
  * Base abstract class for data transformer
  */
trait DataTransformer extends SparkAware {
	
	/**
	  * Abstract method for data transformation
	  * @param data - DataFrame with data to be transformed
	  * @return - DataFrame with transformed data
	  */
	def transform(data: DataFrame): DataFrame
}
