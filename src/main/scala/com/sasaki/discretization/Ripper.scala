package com.sasaki.discretization

import scala.collection.Map

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
		val lineRDD = eitherExtract(rawDatum, opeLookupBc.value)
		lineRDD.persist()
		LogHelper.log(lineRDD.count, "lineRDD count")
		
		/**
		 * Summarize 3 exception types of role's extraction.
		 */
		roleInvaldAnalysis(lineRDD)
		
		/**
		 * Exclude the bad records and repack them as RecordUnit
		 */
		val validLineRDD = validate(lineRDD)
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
		val roleRDD = roleReduce(validLineRDD)
		roleRDD.persist()
		// LogHelper.log(roleRDD.first, "roleRDD")
		LogHelper.log(roleRDD.count, "roleRDD count")
		// val target = roleRDD.filter(_._1 == "2195310412").first
		// LogHelper.log(target, "target")
		
		/**
		 * Retrieve the map ids of team tasks as a local Map
		 */
		val teamMapCode = mapSearch(validLineRDD)
		// LogHelper.log(teamMapCode.take(10), "teamMapCode")
		LogHelper.log(teamMapCode.size, "teamMapCode count")
		// sc.parallelize(teamMapCode.toList, teamMapCodePartitions).saveAsTextFile(mapCounterHDFS)
		val teamMapCodeBc = sc.broadcast(teamMapCode)
		
		/**
		 * Bipartition each role's seq into two parts, personal and team.
		 * Personal part is a whole seq, but the team part is already grouped by maps.
		 */
		val partitionedRoleRDD = bipartition(roleRDD, teamMapCodeBc.value)
		partitionedRoleRDD.persist()
		
		/**
		 * Personal
		 */
		val personalRDD = partitionedRoleRDD.mapValues(_._1).filter(_._2 != None).mapValues(_.get)
			.persist()
		// LogHelper.log(personalRDD.first, "personalRDD")
		LogHelper.log(personalRDD.count, "personalRDD count")
		val roleGradeRDD = personalRDD.map(currySplit(upgradeCode, gradeIndex)(_))
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

  	/**
  	 * A curry function which relies on code and param index for splitting.
  	 */
  	def currySplit(targetEventCode : String, paramIndex : Int)
  			(roleInfo : (String, scala.Seq[RecordUnit])) = {
  		val (role, seqs) = roleInfo
  		val splitIndex = (0 until seqs.size).filter(x => seqs(x).codeMatch(targetEventCode))
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

  	def eitherExtract(rawDatum : RDD[String], opeLookup : Map[String, Seq[Int]]) ={
  		rawDatum.flatMap{ case line =>
			val extractor = new Xtractor(line.trim)
			val code = extractor.motionCodeXtract()
			val timestamp = extractor.timestampXtract()
			val parameters = extractor.paramsXtract()
			val players = extractor.roleIdXtract(code, parameters, opeLookup)
			players.map(p => new RecordEither(p, timestamp, code, parameters))
		}
  	}

  	def validate(lineRDD : RDD[RecordEither]) = {
  		lineRDD.filter(_.isValid).map(_.toKV())
			.mapValues(values => new RecordUnit(values._1, values._2, values._3))
  	}

  	def roleReduce(validLineRDD : RDD[(String, RecordUnit)]) = {
  		validLineRDD.combineByKey(
			(v : RecordUnit) => Seq(v),
			(c : Seq[RecordUnit], v : RecordUnit) => c :+ v,
			(c1 : Seq[RecordUnit], c2 : Seq[RecordUnit]) => c1 ++ c2
		).mapValues(_.toSeq.sortBy(_.timestamp))
  	}

  	def mapSearch(validLineRDD : RDD[(String, RecordUnit)]) = {
  		validLineRDD.values.filter(_.codeMatch(enterMapCode))
			.map(_.parameters.split(",,").last)
			.countByValue
			.filter{ case (_, count) => count > 1 && count < 7}
  	}

  	def bipartition(roleRDD : RDD[(String, Seq[RecordUnit])], teamMapCode : Map[String, Long]) = {
  		roleRDD.mapValues{ seqs =>
  			val pairBuf = collection.mutable.Buffer[(Int, Int, String)]()
			var startAt = seqs.size
			var mapInId = new String()
			(0 until seqs.size).foreach{ i =>
				val role = seqs(i)
				if (role.codeMatch(enterMapCode)) {
					startAt = i
					mapInId = role.parameters.split(",,").last
				} else if (role.codeMatch(leaveMapCode)) {
					val endAt = i
					val mapOutId = role.parameters.split(",,").last
					if (startAt < endAt && mapInId == mapOutId && 
							teamMapCode.contains(mapInId)) {
						pairBuf += Tuple3(startAt, endAt, mapInId)
					}
				}
			}
			if (!pairBuf.isEmpty) {
				val team = pairBuf.map{ case (start, end, id) => (id, seqs.slice(start, end + 1))}.toSeq
				val personal = seqs.zipWithIndex.filter{ case (_, index) => 
					pairBuf.forall{ case (start, end, _) => index > end || index < start}
				}.map(_._1)
				if (!pairBuf.isEmpty) {
					(Some(personal), Some(team))
				} else {
					(None, Some(team))
				}
			} else {
				(Some(seqs), None)
			}
  		}
  	}

}
