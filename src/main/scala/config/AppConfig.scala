package config

import java.io.InputStream
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark.SparkConf

import scala.collection.mutable

/**
  * Loads configuration from properties file and holds it in-memory
  */
object AppConfig {
	
	private val log: Logger = Logger.getLogger(getClass)
	
	//Configuration file
	private val APP_CONFIG_FILE = "app.properties"
	
	//Key constants
	val CONFIG_PROP_INCOME_DATA_FILES_PATH = "app.income.data.files.path"
	val CONFIG_PROP_INCOME_TABLE_NAME = "app.income.table"
	val CONFIG_PROP_INCOME_TALE_FILE_FORMAT = "app.table.file.format"
	val CONFIG_PROP_KIDS_AVG_COLUMN = "app.stats.kids.avg.column"
	val CONFIG_PROP_INCOME_SUM_COLUMN = "app.stats.income.sum.column"
	
	//Configuration parts separated by prefix
	private val SPARK_CONF_PREFIX = "spark."
	private val APP_CONF_PREFIX = "app."
	
	private val sparkConf: SparkConf = new SparkConf()
	private val appConf: mutable.HashMap[String, String] = mutable.HashMap()
	
	//Setters
	private def setSparkConf(key: String, value: String): Unit = sparkConf.set(key, value)
	private def setAppConf(key: String, value: String): Unit = appConf(key) = value
	
	//Getters
	def getSparkConf: SparkConf = sparkConf
	
	/**
	  * Initializes in-memory configuration
	  */
	def init(): Unit = {
		
		val appProperties = new Properties()
		try{
			//Loading configuration from resources folder
			val stream: InputStream = getClass.getResourceAsStream(s"/$APP_CONFIG_FILE")
			appProperties.load(stream)
			stream.close()
		} catch {
			case e: Exception =>
				log.error(s"Could not load application configuration ($APP_CONFIG_FILE): ${e.getMessage}")
				throw e
		}
		
		setupConf(appProperties)
		
		log.info("Configuration loaded")
	}
	
	/**
	  * Private method that stores the configuration in memory
	  * @param properties
	  */
	private def setupConf(properties: Properties): Unit = {
		
		val propsIterator = properties.stringPropertyNames().iterator()
		
		//Iterate over all configuration properties
		while(propsIterator.hasNext){
			val key = propsIterator.next()
			val value = properties.getProperty(key)
			
			log.info(s"$key: $value")
			
			key match {
				case k if k.startsWith(SPARK_CONF_PREFIX) =>
					setSparkConf(key, value)
				case k if k.startsWith(APP_CONF_PREFIX) =>
					setAppConf(key, value)
				case _ => log.warn(s"Configuration property $key cannot be handled")
			}
		}
	}
	
	/**
	  * Returns value of configuration property
	  * @param key Configuration property name
	  * @return Configuration property value
	  */
	def getAppConfAsString(key: String): String = {
		appConf(key)
	}
}
