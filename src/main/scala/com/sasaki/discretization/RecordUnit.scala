package com.sasaki.discretization

import java.util.NoSuchElementException
import java.text.ParseException

import com.sasaki.utils.TimeUtils

class RecordUnit(val timestamp : Long, val motionCode : String, val parameters : String) {

	override def toString() = {
		s"$timestamp@$motionCode@$parameters"
	}

	def codeMatch(code : String) : Boolean = {
		code == motionCode		
	}

	def mapMatch(mapLookup : collection.Map[String, Long]) : Boolean = {
		mapLookup contains parameters.split(",,").last
	}

	def withinScopePairs(pairs : IndexedSeq[(Long, Long)]) : Boolean = {
		pairs.filter(x => timestamp > x._1 && timestamp < x._2).size > 0
	}

}