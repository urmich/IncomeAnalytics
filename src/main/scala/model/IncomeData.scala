package model

/**
  * Class containing data schema definitions
  */
object IncomeData {
	
	val KIDS_COLUMN_IDX = 2
	val INCOME_AMOUNT_COLUMN_IDX = 4
	val INCOME_STR_COULUMN_IDX = 3
	
	//array that defines the data schema
	val COLUMNS: Array[String] = Array("name", "address", "kids", "income", "income_double")
}
