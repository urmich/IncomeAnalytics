package core

import config.AppConfig
import org.apache.spark.sql.SparkSession

/**
  * Responsible for instantiating SparkSession
  * Singleton provides uniqueness of SparkSession in the entire application
  */
object SparkEnabler {
	
	val spark: SparkSession =
		SparkSession
			.builder()
			.config(AppConfig.getSparkConf)
			.enableHiveSupport()
			.getOrCreate()
	
}
