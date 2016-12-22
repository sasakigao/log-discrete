package com.sasaki.discretization

import java.util.NoSuchElementException
import java.text.ParseException

import com.sasaki.utils.TimeUtils

class RecordOption(val role : Option[String], val timestamp : Option[String],
 		val motionCode : Option[String], val parameters : Option[String]) {

	def toKV() = {
		try { 
			Some((role.get, (TimeUtils.toMillis(timestamp.get), motionCode.get, parameters.get)))
		} catch {
			case e: NoSuchElementException => None
			case e: ParseException => None
		}
	}

}