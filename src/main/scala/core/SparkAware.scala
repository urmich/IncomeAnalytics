package core

/**
  * Base for all classes that should have access to SparkSession
  */
trait SparkAware {
	
	//delegating Spark instantiation to designated class
	val spark = SparkEnabler.spark
}
