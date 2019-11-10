package loader

import config.AppConfig
import core.DataWriter
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Class that implements income data writing to Hive table
  */
object IncomeDataWriter extends DataWriter {
	
	/**
	  * Writes data to the INCOME table
	  * @param data - DataFrame that contains all required for saving columns
	  */
	override def write(data: DataFrame): Unit = {
		//writing the data to table
		data
			.write
			.format(AppConfig.getAppConfAsString(AppConfig.CONFIG_PROP_INCOME_TALE_FILE_FORMAT))
			.mode(SaveMode.Overwrite)
			.saveAsTable(AppConfig.getAppConfAsString(AppConfig.CONFIG_PROP_INCOME_TABLE_NAME))
	}
}
