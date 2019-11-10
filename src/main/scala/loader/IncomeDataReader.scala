package loader

import config.AppConfig
import core.DataReader
import org.apache.spark.sql.DataFrame

/**
  * Class that implements reading income data from the CSV files
  */
object IncomeDataReader extends DataReader {
	
	/**
	  * Reads data from the source files
	  * @return DataFrame containing loaded data
	  */
	override def read(): DataFrame = {
		
		//Read data from the source and store in DF
		//Skip CSV files headers in order to prevent filtering
		spark
			.read
			.option("header", "true")
			.csv(AppConfig.getAppConfAsString(AppConfig.CONFIG_PROP_INCOME_DATA_FILES_PATH))
	}
}
