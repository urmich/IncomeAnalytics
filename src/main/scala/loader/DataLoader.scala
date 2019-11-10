package loader

import core.{DataProcessor, DataReader, DataTransformer, DataWriter}
import org.apache.spark.sql._

/**
  * This class implements data loading, transformation and saving into Hive table
  */
object DataLoader extends DataProcessor {
	
	/**
	  * Triggers reading data
	  * @return DataFrame with read data
	  */
	override def read(): DataFrame = {
		
		IncomeDataReader.read()
	}
	
	/**
	  * Triggers data transformation
	  * @param data - DataFrame containing raw data
	  * @return - DataFrame with transformed data
	  */
	override def transform(data: DataFrame): DataFrame = {
	
		IncomeDataTransformer.transform(data)
	}
	
	/**
	  * Triggers writing data
	  * @param data - DataFrame containing data to be written into Hive table
	  */
	override def write(data: DataFrame): Unit = {
	
		IncomeDataWriter.write(data)
	}
}
