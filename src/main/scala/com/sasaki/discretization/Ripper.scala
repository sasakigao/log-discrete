package com.sasaki.discretization

import scala.collection.mutable._

import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}
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
	val personalDataHDFS = "hdfs:///netease/ver2/gen/personal/"
	val teamDataHDFS = "hdfs:///netease/ver2/gen/team/"
	val codeCounterHDFS = "hdfs:///netease/ver2/gen/code-counter/"
	val mapCounterHDFS = "hdfs:///netease/ver2/gen/map-counter/"
	val invalidDataHDFS = "hdfs:///netease/ver2/gen/invalid/"

	// Used constants
	val lookupFilePartitions = 1
	val teamMapCodePartitions = 50
	val codeCounterPartitions = 1
	val gradeIndex = 2
	val mapIndex = 13
	val upgradeCode = "5300125"
	val enterMapCode = "5300055"
	val leaveMapCode = "5300060"

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
		val lineRDD = rawDatum.flatMap{ case line =>
			val extractor = new Xtractor(line.trim)
			val code = extractor.motionCodeXtract()
			val timestamp = extractor.timestampXtract()
			val parameters = extractor.paramsXtract()
			val players = extractor.roleIdXtract(code, parameters, opeLookupBc.value)
			players.map(p => new RecordEither(p, timestamp, code, parameters))
		}
		.persist()
		LogHelper.log(lineRDD.count, "lineRDD count")

		/**
		 * Summarize 3 exception types of role's extraction.
		 */
		roleInvaldAnalysis(lineRDD)
		
		/**
		 * Exclude the bad records and repack them as RecordUnit
		 */
		val validLineRDD = lineRDD.filter(_.isValid).map(_.toKV())
			.mapValues(values => new RecordUnit(values._1, values._2, values._3))
			.persist()
		lineRDD.unpersist()
		LogHelper.log(validLineRDD.first, "validLineRDD")
		LogHelper.log(validLineRDD.count, "validLineRDD count")
		val codeCounter = validLineRDD.map(_._2.motionCode).countByValue.toList.sortBy(-_._2)
		sc.parallelize(codeCounter, codeCounterPartitions).saveAsTextFile(codeCounterHDFS)
		 
		/**
		 * Pack the RecordUnit of one role into a seq and sort it by time.
		 * (role -> Seq[RecordUnit])
		 */
		val roleRDD = validLineRDD.combineByKey(
			(v : RecordUnit) => Seq(v),
			(c : Seq[RecordUnit], v : RecordUnit) => c :+ v,
			(c1 : Seq[RecordUnit], c2 : Seq[RecordUnit]) => c1 ++ c2
		).mapValues(_.toSeq.sortBy(_.timestamp))
		.persist()

		// LogHelper.log(roleRDD.first, "roleRDD")
		LogHelper.log(roleRDD.count, "roleRDD count")
		// val target = roleRDD.filter(_._1 == "2195310412").first
		// LogHelper.log(target, "target")

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
		LogHelper.log(teamMapCode.size, "teamMapCode count")
		// sc.parallelize(teamMapCode.toList, teamMapCodePartitions).saveAsTextFile(mapCounterHDFS)
		
		/**
		 * Seperate the seqs into personal one and team one
		 * Each one contains the pairs of start and end time point
		 * role -> pairs(start, end)
		 */
		val teamTimeScopesRDD = roleRDD.mapValues{ assemblies =>
			val timingEnterMap = assemblies.filter(_.mapMatch(enterMapCode, teamMapCodeBc.value))
			val timingEnterMapSize = timingEnterMap.size
			val timingLeaveMap = assemblies.filter(_.mapMatch(leaveMapCode, teamMapCodeBc.value))
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
		}
		.persist()
		// LogHelper.log(partitionedRoleRDD.count, "partitionedRoleRDD count")
		
		val personalRDD = partitionedRoleRDD.mapValues(_._2).filter(_._2.size >0)
			.persist()
		val teamRDD = partitionedRoleRDD.mapValues(_._1).filter(_._2 != None).mapValues(_.get)
			.persist()
		partitionedRoleRDD.unpersist()
		// LogHelper.log(personalRDD.first, "personalRDD")
		LogHelper.log(personalRDD.count, "personalRDD count")
		// LogHelper.log(teamRDD.first, "teamRDD")
		LogHelper.log(teamRDD.count, "teamRDD count")

		/**
		 * Split personal data by grades
		 * (role, grade) -> seqs
		 */
		val roleGradeRDD = personalRDD.map(currySplit(upgradeCode, gradeIndex)(_))
			.filter(_ != None).map(_.get)
			.flatMap(x => x)
			.persist()
		personalRDD.unpersist()
		// LogHelper.log(roleGradeRDD.first, "roleGradeRDD")
		LogHelper.log(roleGradeRDD.count, "roleGradeRDD count")

		/**
		 * Split person in team data by map id
		 * (role, map) -> seqs
		 */
		val roleGroupRDD = teamRDD.map(currySplit(enterMapCode, mapIndex)(_))
			.filter(_ != None).map(_.get)
			.flatMap(x => x)
			.persist()
		teamRDD.unpersist()
		// LogHelper.log(roleGroupRDD.first, "roleGroupRDD")
		LogHelper.log(roleGroupRDD.count, "roleGroupRDD count")

		StoreFormat.saveAsJSON(roleGradeRDD, personalDataHDFS, sqlContext)
		StoreFormat.saveAsJSON(roleGroupRDD, teamDataHDFS, sqlContext)

		roleGradeRDD.unpersist()
		teamRDD.unpersist()
    		sc.stop()
  	}

  	/**
  	 * A curry function which relies on code and param index for splitting.
  	 */
  	def currySplit(targetEventCode : String, paramIndex : Int)
  			(roleInfo : (String, scala.Seq[RecordUnit])) = {
  		val (role, seqs) = roleInfo
  		val splitIndex = (0 until seqs.size).filter(x => seqs(x).codeMatch(targetEventCode) || seqs(x).firstEnterMap)
		// only those appear more than once are considered
		if (splitIndex.size > 1) {
			val splits = splitIndex.sliding(2, 1).map{ headAndToe =>
				seqs.slice(headAndToe.head, headAndToe.last)
			}.toSeq
			val result = splits.map{ split =>
				val mark = split.head.parameters.split(",")(paramIndex)
				Row(role, mark, split.map(_.toString))
			}
			Some(result)
		} else {
			None
		}
  	}

  	def roleInvaldAnalysis(lineRDD : RDD[RecordEither]) = {
  		val roleInvalidRDD = lineRDD.filter(_.isRoleInvalid).persist()
		val roleDependentInvalidRDD = lineRDD.filter(_.isRoleDependentInvalid).persist()
		val roleUnexpectedRDD = lineRDD.filter(_.isRoleUnexpected).persist()
		LogHelper.log(roleInvalidRDD.count, "roleInvalidRDD count")
		LogHelper.log(roleDependentInvalidRDD.count, "roleDependentInvalidRDD count")
		LogHelper.log(roleUnexpectedRDD.count, "roleUnexpectedRDD count")
		roleUnexpectedRDD.takeSample(false, 100, 11L).foreach(LogHelper.log(_, "roleUnexpectedRDD 100"))
		roleDependentInvalidRDD.coalesce(10, false).saveAsTextFile(invalidDataHDFS + "role-dep/")
		roleUnexpectedRDD.coalesce(10, false).saveAsTextFile(invalidDataHDFS + "role-unexp/")
  	}

}
