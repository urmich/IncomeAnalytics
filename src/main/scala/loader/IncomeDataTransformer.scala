package loader

import core.DataTransformer
import model.IncomeData
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
  * Class that implements income data transformation
  */
object IncomeDataTransformer extends DataTransformer {
	
	//Registering UDF function that drops the '$' sign from the amount and converts it to double
	val stringAmountToDouble = udf((amount: String) => amount.substring(1, amount.length).toDouble)
	
	/**
	  * Transforms data to the required structure. Prepares data for saving into Hive table
	  *
	  * @param rawData - DataFrame containing the raw data from the files
	  * @return DataFrame containing transformed data
	  */
	override def transform(rawData: DataFrame): DataFrame = {
		
		//Add to DF additional "calculated" column with values of income without currency sign
		//This additional column will be saved in Hive table for further usage
		
		rawData
			.withColumn(
				IncomeData.COLUMNS(IncomeData.INCOME_AMOUNT_COLUMN_IDX),
				stringAmountToDouble(lit(col(IncomeData.COLUMNS(IncomeData.INCOME_STR_COULUMN_IDX))))
			)
	}
}
