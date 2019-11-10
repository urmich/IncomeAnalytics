package common

import core.DataWriter
import org.apache.spark.sql.DataFrame

object StdoutDataWriter extends DataWriter {
	/**
	  * Method for writing data.
	  * Prints the input DataFrame to STDOUT
	  *
	  * @param data - DataFrame with data to be written
	  */
	override def write(data: DataFrame): Unit = {
	
		//print DataFrame to STDOUT
		data.show()
	}
}
