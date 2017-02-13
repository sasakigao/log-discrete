package com.sasaki.discretization

import scala.collection.Map

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}

import com.sasaki.utils._


/**
 * Stage 1 -- Discretization
 */
object Ripper {
	val appName = "Sasaki the Ripper"

    	// Raw data files to read
	// val playerLogHDFS = "hdfs:///netease/ver1/datum/labelled/"
	val playerLogHDFS = "hdfs:///netease/ver2/datum/"
    	// Motions lookup file to read
	val motionsLookupFile = "hdfs:///netease/ver2/operations-user"

	// Write to
	val personalDataHDFS = "hdfs:///netease/ver2/gen/2/personal/"
	val teamDataHDFS = "hdfs:///netease/ver2/gen/2/team/"
	val codeCounterHDFS = "hdfs:///netease/ver2/gen/2/code-counter/"
	val mapCounterHDFS = "hdfs:///netease/ver2/gen/2/map-counter/"
	val invalidDataHDFS = "hdfs:///netease/ver2/gen/2/invalid/"

	// Used constants
	val lookupFilePartitions = 1
	val teamMapCodePartitions = 20
	val codeCounterPartitions = 1
	val gradeIndex = 2
	val mapIndex = 13
	val upgradeCode = "5300125"

	def main(args: Array[String]) = {
	  	val confSpark = new SparkConf().setAppName(appName)
	  	confSpark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	  	confSpark.registerKryoClasses(Array(classOf[Xtractor], classOf[RecordUnit], classOf[RecordEither]))
  		val sc = new SparkContext(confSpark)
		val sqlContext = new SQLContext(sc)
  		sc.setLogLevel("WARN")

		val opeLookup = sc.textFile(motionsLookupFile, lookupFilePartitions)
			.map{x => val f = x.split(","); (f(0), f(1))}
			.groupByKey
			.collectAsMap
			.mapValues(_.map(_.toInt).toSeq)
		val opeLookupBc = sc.broadcast(opeLookup)

		val rawDatum = sc.textFile(playerLogHDFS)
		// .zipWithIndex
		
		/**
		 * Extract the fields as options.
		 * Per line might generate two records for it involves two roles.
		 */
		val lineRDD = ClosureFunc.eitherExtract(rawDatum, opeLookupBc)
		lineRDD.persist()
		LogHelper.log(lineRDD.count, "lineRDD count")
		
		/**
		 * Summarize 3 exception types of role's extraction.
		 */
		ClosureFunc.roleInvaldAnalysis(lineRDD, invalidDataHDFS)
		
		/**
		 * Exclude the bad records and repack them as RecordUnit
		 */
		val validLineRDD = ClosureFunc.validate(lineRDD)
		lineRDD.unpersist()
		validLineRDD.persist()
		// LogHelper.log(validLineRDD.first, "validLineRDD")
		LogHelper.log(validLineRDD.count, "validLineRDD count")

		/**
		 * Count each motion and save
		 */
		val codeCounter = validLineRDD.map(_._2.motionCode).countByValue.toList.sortBy(-_._2)
		sc.parallelize(codeCounter, codeCounterPartitions).saveAsTextFile(codeCounterHDFS)
		 
		/**
		 * Pack the RecordUnit of one role into a seq and sort it by time.
		 * (role -> Seq[RecordUnit])
		 */
		val roleRDD = ClosureFunc.roleReduce(validLineRDD)
		roleRDD.persist()
		// LogHelper.log(roleRDD.first, "roleRDD")
		LogHelper.log(roleRDD.count, "roleRDD count")
		// val target = roleRDD.filter(_._1 == "2195310412").first
		// LogHelper.log(target, "target")
		
		/**
		 * Retrieve the map ids of team tasks as a local Map
		 */
		val teamMapCode = ClosureFunc.mapSearch(validLineRDD)
		// LogHelper.log(teamMapCode.take(10), "teamMapCode")
		LogHelper.log(teamMapCode.size, "teamMapCode count")
		// sc.parallelize(teamMapCode.toList, teamMapCodePartitions).saveAsTextFile(mapCounterHDFS)
		val teamMapCodeBc = sc.broadcast(teamMapCode)
		
		/**
		 * Bipartition each role's seq into two parts, personal and team.
		 * Personal part is a whole seq, but the team part is already grouped by maps.
		 */
		val partitionedRoleRDD = ClosureFunc.bipartition(roleRDD, teamMapCodeBc.value)
		partitionedRoleRDD.persist()
		
		/**
		 * Personal
		 */
		val personalRDD = partitionedRoleRDD.mapValues(_._1).filter(_._2 != None).mapValues(_.get)
			.persist()
		// LogHelper.log(personalRDD.first, "personalRDD")
		LogHelper.log(personalRDD.count, "personalRDD count")
		val roleGradeRDD = personalRDD.map(ClosureFunc.currySplit(upgradeCode, gradeIndex)(_))
			.filter(_ != None).map(_.get)
			.flatMap(x => x)
			.persist()
		personalRDD.unpersist()
		// LogHelper.log(roleGradeRDD.first, "roleGradeRDD")
		LogHelper.log(roleGradeRDD.count, "roleGradeRDD count")

		/**
		 * Team
		 */
		val roleGroupRDD = partitionedRoleRDD.mapValues(_._2).filter(_._2 != None).mapValues(_.get)
			.flatMap{ case (role, teams) => teams.map(team => Row(role, team._1, team._2))}
			.persist()
		// LogHelper.log(roleGroupRDD.first, "roleGroupRDD")
		LogHelper.log(roleGroupRDD.count, "roleGroupRDD count")

		StoreFormat.saveAsJSON(roleGradeRDD, personalDataHDFS, sqlContext)
		StoreFormat.saveAsJSON(roleGroupRDD, teamDataHDFS, sqlContext)
    		sc.stop()
  	}

}
