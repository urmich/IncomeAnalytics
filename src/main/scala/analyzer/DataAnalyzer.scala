package analyzer

import common.StdoutDataWriter
import core.DataProcessor
import org.apache.spark.sql.DataFrame

object DataAnalyzer extends DataProcessor {
	
	/**
	  * Method for orchestration of reading data
	  *
	  * @return DataFrame with read data
	  */
	override def read(): DataFrame = {
		
		DataAnalyzerReader.read()
	}
	
	/**
	  * Method for orchestration of transforming data
	  *
	  * @param data - DataFrame with data to be transformed
	  * @return - DataFrame with data after transformation
	  */
	override def transform(data: DataFrame): DataFrame = {
		
		DataAnalyzerTransformer.transform(data)
	}
	
	/**
	  * Method for orchestration of writing data
	  *
	  * @param data - DataFrame with data to be written
	  */
	override def write(data: DataFrame): Unit = {
		//Use specific implementation of DataWriter (no specifc requirements were provided)
		StdoutDataWriter.write(data)
	}
}
