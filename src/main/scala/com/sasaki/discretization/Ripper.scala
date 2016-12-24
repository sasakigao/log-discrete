package com.sasaki.discretization

import scala.collection.mutable._
import scala.collection.Iterable

import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

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
	val motionsLookupFile = "hdfs:///netease/ver1/datum/operations-user"

	// Write to
	val personalDataHDFS = "hdfs:///netease/ver2/gen/personal/"
	val teamDataHDFS = "hdfs:///netease/ver2/gen/team/"
	val codeCounterHDFS = "hdfs:///netease/ver2/gen/code-counter/"
	val mapCounterHDFS = "hdfs:///netease/ver2/gen/map-counter/"

	// Used constants
	val lookupFilePartitions = 1
	val teamMapCodePartitions = 50
	val codeCounterPartitions = 1
	val isPositive = 1
	val isNegative = 0
	val upgradeCode = "5300125"
	val enterMapCode = "5300055"
	val leaveMapCode = "5300060"

	def main(args: Array[String]) = {
	  	val confSpark = new SparkConf().setAppName(appName)
	  	confSpark.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	  	confSpark.registerKryoClasses(Array(classOf[Xtractor], classOf[RecordUnit], classOf[RecordOption]))
  		val sc = new SparkContext(confSpark)
  		sc.setLogLevel("WARN")

		val opeLookup = sc.textFile(motionsLookupFile, lookupFilePartitions).collect
			.map{x => val f = x.split(","); (f(0), f(1))}
			.toMap
		val opeLookupBc = sc.broadcast(opeLookup)

		val rawDatum = sc.textFile(playerLogHDFS)
		
		/**
		 * Extract the fields as options
		 */
		val lineRDD = rawDatum.map{ line =>
			val extractor = new Xtractor(line)
			val code = extractor.motionCodeXtract()
			val timestamp = extractor.timestampXtract()
			val player = extractor.roleIdXtract(code, opeLookupBc.value)
			val parameters = extractor.paramsXtract()
			new RecordOption(player, timestamp, code, parameters)
		}.persist()
		// LogHelper.log(lineRDD.count, "lineRDD count")

		/**
		 * Exclude the bad records and repack them as RecordUnit
		 */
		val validLineRDD = lineRDD.map(_.toKV()).filter(_ != None).map(_.get)
			.mapValues(values => new RecordUnit(values._1, values._2, values._3))
			.persist()
		lineRDD.unpersist()
		// LogHelper.log(validLineRDD.first, "validLineRDD")
		// LogHelper.log(validLineRDD.count, "validLineRDD count")
		// val codeCounter = validLineRDD.map(_._2.motionCode).countByValue.toList.sortBy(-_._2)
		// sc.parallelize(codeCounter, codeCounterPartitions).saveAsTextFile(codeCounterHDFS)
		 
		/**
		 * Pack the RecordUnit of one role into a seq and sort it by time.
		 * (role -> seqs)
		 */
		val roleRDD = validLineRDD.groupByKey.
			mapValues(_.toSeq.sortBy(_.timestamp)).persist()
		// LogHelper.log(roleRDD.first, "roleRDD")
		// LogHelper.log(roleRDD.count, "roleRDD count")

		/**
		 * Retrieve the map codes of team tasks
		 */
		val teamMapCode = validLineRDD.values.filter(_.codeMatch(enterMapCode))
			.map(_.parameters.split(",,").last)
			.countByValue
			.filter{ case (_, count) => count > 1 && count < 7}
		validLineRDD.unpersist()
		val teamMapCodeBc = sc.broadcast(teamMapCode)
		// LogHelper.log(teamMapCode.take(10), "teamMapCode")
		// LogHelper.log(teamMapCode.size, "teamMapCode count")
		// sc.parallelize(teamMapCode.toList, teamMapCodePartitions).saveAsTextFile(mapCounterHDFS)
		
		/**
		 * Seperate the seqs into personal one and team one
		 * Each one contains the pairs of start and end time point
		 * role -> pairs(start, end)
		 */
		val teamTimeScopesRDD = roleRDD.mapValues{ assemblies =>
			val timingEnterMap = assemblies.filter(_.codeMatch(enterMapCode))
				.filter(_.mapMatch(teamMapCodeBc.value))
			val timingEnterMapSize = timingEnterMap.size
			val timingLeaveMap = assemblies.filter(_.codeMatch(leaveMapCode))
				.filter(_.mapMatch(teamMapCodeBc.value))
			val timingLeaveMapSize = timingLeaveMap.size
			if (timingEnterMapSize * timingLeaveMapSize > 0) {          // start before a enter
				if (timingEnterMap.head.timestamp > timingLeaveMap.head.timestamp) {
					val intervalsNumber = Math.min(timingEnterMapSize, timingLeaveMapSize)
					val pairs = (0 until intervalsNumber)
						.map(x => (timingEnterMap(x).timestamp, timingLeaveMap(x).timestamp))
					Some(pairs)
				} else {                                                 // start before a leave
					val headOffTimingLeaveMap = timingLeaveMap.tail
					val intervalsNumber = Math.min(timingEnterMapSize, headOffTimingLeaveMap.size)
					val pairs = (0 until intervalsNumber)
						.map(x => (timingEnterMap(x).timestamp, headOffTimingLeaveMap(x).timestamp))
					Some(pairs)
				}
			} else {
				None
			}
		}

		/**
		 * Use the time pairs to split each seqs
		 */
		val scopesJoinLinesRDD = roleRDD.join(teamTimeScopesRDD)
		val partitionedRoleRDD = scopesJoinLinesRDD.map{ case (role, (assemblies, scopePairs)) =>
			if (scopePairs != None) {                                                    // this role has team parts
				val (teamParts, personalParts) = assemblies.partition(_.withinScopePairs(scopePairs.get))
				(role, (Some(teamParts), personalParts))
			} else {
				(role, (None, assemblies))
			}
		}.persist()
		LogHelper.log(partitionedRoleRDD.count, "partitionedRoleRDD count")

		
		val personalRDD = partitionedRoleRDD.mapValues(_._2).persist()
		val teamRDD = partitionedRoleRDD.mapValues(_._1).filter(_._2 != None).mapValues(_.get).persist()
		partitionedRoleRDD.unpersist()
		LogHelper.log(personalRDD.first, "personalRDD")
		LogHelper.log(personalRDD.count, "personalRDD count")
		LogHelper.log(teamRDD.first, "teamRDD")
		LogHelper.log(teamRDD.count, "teamRDD count")

		/**
		 * Split by grades
		 * (role, grade) -> seqs
		 */
		val roleGradeRDD = personalRDD.map{ case (role, seqs) =>
			val upgradeIndex = (0 until seqs.size).filter(seqs(_).codeMatch(upgradeCode))
			if (upgradeIndex.size > 1) {                                             // only those upgrade more than once are considered
				val splits = upgradeIndex.sliding(2, 1).map{ headAndToe =>
					seqs.slice(headAndToe.head, headAndToe.last)
				}
				val result = splits.map{ split =>
					val grade = split.head.parameters.split(",,")(1).toInt
					((role, grade), split)
				}
				Some(result)
			} else {
				None
			}
		}.filter(_ != None).map(_.get)
		.flatMap(x => x)
		.persist()
		personalRDD.unpersist()
		LogHelper.log(roleGradeRDD.first, "roleGradeRDD")
		LogHelper.log(roleGradeRDD.count, "roleGradeRDD count")


		// roleGradeRDD.saveAsObjectFile(personalDataHDFS)
		// teamRDD.saveAsObjectFile(teamDataHDFS)
		roleGradeRDD.saveAsTextFile(personalDataHDFS)
		teamRDD.saveAsTextFile(teamDataHDFS)

		roleGradeRDD.unpersist()
		teamRDD.unpersist()
    		sc.stop()
  	}

}
