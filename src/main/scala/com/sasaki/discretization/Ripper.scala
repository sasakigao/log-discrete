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
	val playerLogHDFS = "hdfs:///netease/ver2/datum/"
    	// Motions lookup file to read
	val motionsLookupFile = "hdfs:///netease/ver2/motions-lookup"

	// Write to
	val personalDataHDFS = "hdfs:///netease/ver2/personal/"
	val teamDataHDFS = "hdfs:///netease/ver2/team/"

	// Used constants
	val lookupFilePartitions = 1
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

		val opeLookup = sc.textFile(motionsLookupFile, lookupFilePartitions).collect
			.map{x => val f = x.split(","); (f(0), f(1))}
			.toMap
		val opeLookupBc = sc.broadcast(opeLookup)

		val rawDatum = sc.textFile(playerLogHDFS)
		
		// Extract the fields as options
		val lineRDD = rawDatum.map{ line =>
			val extractor = new Xtractor(line)
			val code = extractor.motionCodeXtract()
			val timestamp = extractor.timestampXtract()
			val player = extractor.roleIdXtract(code, opeLookupBc.value)
			val parameters = extractor.paramsXtract()
			new RecordOption(player, timestamp, code, parameters)
		}
		
		// Exclude the bad records and repack them as RecordUnit
		val validLineRDD = lineRDD.map(_.toKV()).filter(_ != None).map(_.get)
			.mapValues(values => new RecordUnit(values._1, values._2, values._3))
		 
		// Pack the RecordUnit of one role into a seq.
		// (role -> seqs)
		val roleRDD = validLineRDD.groupByKey.mapValues(_.toSeq).persist()

		// Retrieve the map codes of team tasks
		val teamMapCode = validLineRDD.values.filter(_.codeMatch(enterMapCode))
			.map(_.motionCode.split(",,").last)
			.countByValue
			.filter{ case (_, count) => count > 1 && count < 7}
		val teamMapCodeBc = sc.broadcast(teamMapCode)

		// Seperate the seqs into personal one and team one
		// Each one contains the pairs of start and end time point
		val teamTimeScopes = roleRDD.mapValues{ assemblies =>
			val timingLines = assemblies.sortBy(_.timestamp)
			val timingEnterMap = timingLines.filter(_.codeMatch(enterMapCode))
				.filter(_.mapMatch(teamMapCodeBc.value))
			val timingEnterMapSize = timingEnterMap.size
			val timingLeaveMap = timingLines.filter(_.codeMatch(leaveMapCode))
				.filter(_.mapMatch(teamMapCodeBc.value))
			val timingLeaveMapSize = timingLeaveMap.size
			if (timingEnterMapSize * timingLeaveMapSize > 0) {          // start before a enter
				if (timingEnterMap.head.timestamp > timingLeaveMap.head.timestamp) {
					val intervalsNumber = Math.min(timingEnterMapSize, timingLeaveMapSize)
					val pairs = (0 until intervalsNumber)
						.map(x => (timingEnterMap(x).timestamp, timingLeaveMap(x).timestamp))
					Some(pairs)
				} else {                                                 // start before a leave
					val headOffTimingLeaveMap = timingLeaveMap.slice(1, timingLeaveMapSize)
					val intervalsNumber = Math.min(timingEnterMapSize, headOffTimingLeaveMap.size)
					val pairs = (0 until intervalsNumber)
						.map(x => (timingEnterMap(x).timestamp, headOffTimingLeaveMap(x).timestamp))
					Some(pairs)
				}
			} else {
				None
			}
		}.collect.toMap
		val teamTimeScopesBc = sc.broadcast(teamTimeScopes)

		// Use the time pairs to split each seqs
		val partitionedRoleRDD = roleRDD.map{ case (role, assemblies) =>
			val pairs = (teamTimeScopesBc.value)(role)
			if (pairs != None) {                                                    // this role has team parts
				val (teamParts, personalParts) = assemblies.partition(_.withinScopePairs(pairs.get))
				(role, (Some(teamParts), personalParts))
			} else {
				(role, (None, assemblies))
			}
		}.persist()
		val personalRDD = partitionedRoleRDD.mapValues(_._2)
		val teamRDD = partitionedRoleRDD.mapValues(_._1).filter(_._2 != None).mapValues(_.get)

		// (role, grade) -> seqs
		val roleGradeRDD = personalRDD.flatMap{ case (role, seqs) =>
			val upgradeIndex = (0 until seqs.size).filter(seqs(_).codeMatch(upgradeCode))
			val splits = upgradeIndex.sliding(2, 1).map{ headAndToe =>
				seqs.slice(headAndToe.head, headAndToe.last)
			}
			splits.map{ split =>
				val grade = split.head.parameters.split(",,")(1).toInt
				((role, grade), split)
			}
		}


		// roleGradeRDD.saveAsObjectFile(personalDataHDFS)
		// teamRDD.saveAsObjectFile(teamDataHDFS)
		roleGradeRDD.saveAsTextFile(personalDataHDFS)
		teamRDD.saveAsTextFile(teamDataHDFS)

    		sc.stop()
  	}

}
