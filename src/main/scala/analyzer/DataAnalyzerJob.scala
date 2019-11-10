package analyzer

import config.AppConfig

/**
* Entry point into JOB that analyzes income data saved in the Hive Table
*/
object DataAnalyzerJob {
	
	/**
	  * Main method
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		
		//Initializing configuration
		AppConfig.init()
		
		//trigger data loading into Hive table
		DataAnalyzer.process()
	}
}
