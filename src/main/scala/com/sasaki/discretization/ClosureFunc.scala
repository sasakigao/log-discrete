package com.sasaki.discretization

import scala.collection.Map

import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, Row}
import org.apache.spark.broadcast.Broadcast

import com.sasaki.utils._


object ClosureFunc {

	val enterMapCode = "5300055"
	val leaveMapCode = "5300060"

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

  	def roleInvaldAnalysis(lineRDD : RDD[RecordEither], invalidDataHDFS : String) = {
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

  	def eitherExtract(rawDatum : RDD[String], opeLookup : Broadcast[Map[String, Seq[Int]]]) ={
  		rawDatum.flatMap{ case line =>
			val extractor = new Xtractor(line.trim)
			val code = extractor.motionCodeXtract()
			val timestamp = extractor.timestampXtract()
			val parameters = extractor.paramsXtract()
			val players = extractor.roleIdXtract(code, parameters, opeLookup.value)
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
