package analyzer

import config.AppConfig
import core.DataReader
import model.IncomeData
import org.apache.spark.sql.DataFrame

object DataAnalyzerReader extends DataReader{
	
	/**
	  * Method for reading data from the Hive table
	  *
	  * @return DataFrame with read data
	  */
	override def read(): DataFrame = {
		this.selectKidsAndIncome()
	}
	
	/**
	  * private method to get required data fromt he Hive table
	  * @return DataFrame with only kinds and income columns
	  */
	private def selectKidsAndIncome(): DataFrame = {
		
		val kidsColumn = IncomeData.COLUMNS(IncomeData.KIDS_COLUMN_IDX)
		val incomeColumn = IncomeData.COLUMNS(IncomeData.INCOME_AMOUNT_COLUMN_IDX)
		val tableName = AppConfig.getAppConfAsString(AppConfig.CONFIG_PROP_INCOME_TABLE_NAME)
		
		//сselect from table only required columns for better performance
		spark
    		.sql(s"select $kidsColumn, $incomeColumn from $tableName")
	}
}
