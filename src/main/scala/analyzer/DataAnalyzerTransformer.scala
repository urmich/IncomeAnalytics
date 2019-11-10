package analyzer

import config.AppConfig
import core.DataTransformer
import model.IncomeData
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataAnalyzerTransformer extends DataTransformer {
	/**
	  * Method for data transformation
	  *
	  * @param data - DataFrame with data to be transformed
	  * @return - DataFrame with transformed data
	  */
	override def transform(data: DataFrame): DataFrame = {
		this.calculateIncomeStats(data)
	}
	
	/**
	  * Private method that calculates Kids average and some of Incomes
	  *
	  * @param data
	  * @return DataFrame with Kids average and sum of Incomes
	  */
	private def calculateIncomeStats(data: DataFrame): DataFrame = {
		
		val roundingFactor = 2
		
		data
			//this calculates the required stats
			.select(
			round(avg(col(IncomeData.COLUMNS(IncomeData.KIDS_COLUMN_IDX))), roundingFactor),
			round((sum(col(IncomeData.COLUMNS(IncomeData.INCOME_AMOUNT_COLUMN_IDX)))), roundingFactor)
		)
			//rename column names to more readable as preparation to stdout
			.toDF(
			AppConfig.getAppConfAsString(AppConfig.CONFIG_PROP_KIDS_AVG_COLUMN),
			AppConfig.getAppConfAsString(AppConfig.CONFIG_PROP_INCOME_SUM_COLUMN)
		)
	}
}
