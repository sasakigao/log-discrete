package com.sasaki.discretization

import util.control.Exception

import com.sasaki.utils._

class RecordEither(val role : Either[(Int, String), String], 
		val timestamp : Either[(Int, String), String],
 		val motionCode : Either[(Int, String), String], 
 		val parameters : Either[(Int, String), String]) {

	val isValid : Boolean = role.isRight && timestamp.isRight && 
		motionCode.isRight && parameters.isRight

	val isTimeInvalid : Boolean = timestamp.isLeft
	val isMotionInvalid : Boolean = motionCode.isLeft
	val isParamsInvalid : Boolean = parameters.isLeft

	// Use two variables to describe invalid role.
	// One is caused by expected normal error while the other one by expected.
	val isRoleInvalid : Boolean = role.isLeft &&
		 role.left.get._1 != Xtractor.errCodeElse
	val isRoleUnexpected : Boolean = role.isLeft &&
		 role.left.get._1 == Xtractor.errCodeElse

	override def toString() = {
		if (isValid) {
			s"${role}@${timestamp}@${motionCode}@${parameters}"
		} else {
			val eithers = Array(role, timestamp, motionCode, parameters)
			val lefts = eithers.filter(_.isLeft)
			val errCodes = lefts.map(_.left.get._1).mkString("@")
			s"${errCodes}@${lefts.head.left.get._2}"
		}
	}

	/**
	 * Transform if valid. Or throw exception.
	 */
	def toKV() = {
		if (isValid)
			(role.right.get, (TimeUtils.toMillis(timestamp.right.get), 
				motionCode.right.get, parameters.right.get))
		else
			throw new Exception
	}

}