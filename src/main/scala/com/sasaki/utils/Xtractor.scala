package com.sasaki.utils

import util.Either
import util.control.Exception
import collection.Map

class Xtractor(val logLine : String) {

	def motionCodeXtract() : Either[(Int, String), String] = {
		val pattern = """INFO\|[A-Z]{1,10}\|\[[0-9]{7}""".r       // judged by INFO
  		val matchRes = pattern.findFirstIn(logLine)
  		if (matchRes != None) 
  			Right(matchRes.get.split('[').last)
  		else 
  			Left((Xtractor.motionErrCode, logLine))
	}

	def timestampXtract() : Either[(Int, String), String] = {
		val pattern = """^2016-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}""".r
  		val matchRes = pattern.findFirstIn(logLine)
  		if (matchRes != None) 
  			Right(matchRes.get) 
  		else 
  			Left((Xtractor.timestampErrCode, logLine))
	}

	def paramsXtract() : Either[(Int, String), String] = {
		val pattern = """INFO\|[A-Z]{1,10}\|\[[0-9]{7}\].*""".r
  		val matchRes = pattern.findFirstIn(logLine)
  		if (matchRes != None) 
  			Right(matchRes.get.split(']').last.trim) 
  		else 
  			Left((Xtractor.paramsErrCode, logLine))
	}

	/**
	 * Depending on valid code and params split in the 1st if/else.
	 * Then whether the code is included in the list is the 2nd layer.
	 * If included, whether is sp split in the 3rd layer.
	 * Moreover, role exception includes params and code one.
	 */
	def roleIdXtract(codeMaybe : Either[(Int, String), String], 
			paramsMaybe : Either[(Int, String), String], 
  			opeLookup : Map[String, Seq[Int]]) : Seq[Either[(Int, String), String]] = {
		if (codeMaybe.isRight && paramsMaybe.isRight) {
			val code = codeMaybe.right.get
			val params = paramsMaybe.right.get
			val roles : Seq[String] = if (opeLookup contains code) {
				val index = opeLookup.get(code).get
				val paramsArr = params.split(",,")
				index.map(x => if (paramsArr.size > x) paramsArr(x) else Xtractor.indexErrCode.toString)
			} else {
				spCodeHandle(code, params)
			}
			// 3 cases may appear in roles.
			// Correct code, designated err code and unknown messy code.
			roles.map{ role =>
				val normalPat = """^[0-9]{7,12}$""".r
				val normalRes = normalPat.findFirstIn(role)
				val knownErrPat = """^[0-9]{3}$""".r
				val either : Either[(Int, String), String] = 
					if (normalRes != None) {
						Right(normalRes.get.toLong.toString)           // handle the case like "0123456"
					} else if (knownErrPat.findFirstIn(role) != None) {
						Left((role.toInt, logLine))
					} else {
						Left((Xtractor.messyRoleCode, logLine))
					}
				either
			}
		} else {
			Seq(Left((Xtractor.dependentErrCode, logLine)))
		}
	}

	def spCodeHandle(code : String, params : String) : Seq[String] = {
		code match {
			case "5300210" => 
				val paramsArr = params.split(",,")
				if (logLine.split("\\[").size == 5) 
					Seq(paramsArr.head) 
				else if (paramsArr.size > 2)
					Seq(paramsArr(1))
				else 
					Seq(Xtractor.errCode5300210.toString)
			case "5300510" => 
				val p = """playerId=[0-9]{7,12}""".r
				val res = p.findFirstIn(params)
				if (res != None)
  					Seq(res.get.split("=").last)
  				else
  					Seq(Xtractor.errCode5300510.toString)
			case "5300581" => 
				val paramsArr = params.split(" ")
				if (paramsArr.size > 2) 
					Seq(paramsArr(1))
				else
					Seq(Xtractor.errCode5300581.toString)
			case "5300585" => 
				val infoPat = """INFO\|[A-Z]{4,6}""".r
				val afterInfo = infoPat.findFirstIn(logLine)
				if (afterInfo != None) {
					if (afterInfo.get == "SALE") {
						val p = """PlayerId=[0-9]{7,12}""".r
						val res = p.findFirstIn(params)
						if (res != None)
	  						Seq(res.get.split("=").last)
	  					else
							Seq(Xtractor.errCode5300585.toString)
					} else if (afterInfo.get == "PLAYER") {
						Seq(params.split(",,").head)
					} else {
						Seq(Xtractor.errCode5300585.toString)
					}
				} else {
					Seq(Xtractor.errCode5300585.toString)
				}
			case "5300586" => 
				val infoPat = """INFO\|[A-Z]{4,6}""".r
				val afterInfo = infoPat.findFirstIn(logLine)
				if (afterInfo != None) {
					if (afterInfo.get == "SALE") {
						val p1 = """SellerId=[0-9]{7,12}""".r
						val p2 = """BuyerId=[0-9]{7,12}""".r
						val res1 = p1.findFirstIn(params)
						val res2 = p2.findFirstIn(params)
						if (res1 != None && res2 != None)
	  						Seq(res1.get.split("=").last, res2.get.split("=").last)
	  					else
							Seq(Xtractor.errCode5300586.toString)
					} else if (afterInfo.get == "PLAYER") {
						Seq(params.split(",,").head)
					} else {
						Seq(Xtractor.errCode5300586.toString)
					}
				} else {
					Seq(Xtractor.errCode5300586.toString)
				}
  			case _ => Seq(Xtractor.errCodeElse.toString)
  		}
	}

}

object Xtractor {
	val motionErrCode = 101
	val timestampErrCode = 102
	val roleErrCode = 103
	val paramsErrCode = 104
	val codeErrCode = 105
	val dependentErrCode = 106
	val excludedCodeErrCode = 107
	val errCode5300210 = 108
	val errCode5300510 = 109
	val errCode5300581 = 110
	val errCode5300585 = 111
	val errCode5300586 = 112
	val errCodeElse = 113
	val indexErrCode = 114
	val messyRoleCode = 115
	
	def apply(logLine : String) = {
		new Xtractor(logLine)
	}
}