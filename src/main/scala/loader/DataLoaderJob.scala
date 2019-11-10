package loader

import config.AppConfig

/**
  * Entry point into JOB that loads income data from source files and saves it into Hive table
  */
object DataLoaderJob {
  
  /**
    * Main method
    * @param args
    */
  def main(args: Array[String]): Unit = {

    //Initializing configuration
    AppConfig.init()
    
    //trigger data loading into Hive table
    DataLoader.process()
  }
}
