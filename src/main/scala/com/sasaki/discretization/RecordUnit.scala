package com.sasaki.discretization

import java.util.NoSuchElementException
import java.text.ParseException

import com.sasaki.utils.TimeUtils

class RecordUnit(val timestamp : Long, 
		val motionCode : String, 
		val parameters : String) {

	override def toString() = {
		s"$timestamp@$motionCode@$parameters"
	}

	def codeMatch(code : String) : Boolean = {
		code == motionCode
	}

}