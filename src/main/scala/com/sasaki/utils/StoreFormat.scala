package com.sasaki.utils

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.rdd.RDD

import com.sasaki.discretization._

object StoreFormat {

	def rdd2DF(rdd : RDD[Row], sqlContext : SQLContext) = {
    		val schema = StructType(
    			StructField("role", StringType, nullable = false) ::
    			StructField("mark", StringType, nullable = false) ::
			StructField("seqs", ArrayType(StringType), nullable = false) :: 
			Nil)
		sqlContext.createDataFrame(rdd, schema)		
	}

	def saveAsJSON(rdd : RDD[Row], 
			path : String, sqlContext : SQLContext) = {
		val df = rdd2DF(rdd, sqlContext)
		val saveOptions = Map("header" -> "false", "path" -> path)
    		df.write.format("json").mode(SaveMode.Ignore).options(saveOptions).save
	}
	
}