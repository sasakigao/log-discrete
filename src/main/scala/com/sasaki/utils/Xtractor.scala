package com.sasaki.utils

import collection.immutable.{Map => imMap}

class Xtractor(val logLine : String) {

	def motionCodeXtract() : Option[String] = {
		val pattern = """(PLAYER|LOGIN)\|\[[0-9]{7}""".r             // PLAYER or LOGIN
  		val matchRes = pattern.findFirstIn(logLine)
  		if (matchRes != None) Some(matchRes.get.split('[').last) else None
	}

	def timestampXtract() : Option[String] = {
		val pattern = """^2016-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}""".r
  		val matchRes = pattern.findFirstIn(logLine)
  		if (matchRes != None) Some(matchRes.get.split('[').last) else None
	}

	def roleIdXtract(opeId : Option[String], 
  			opeLookup : imMap[String, String]) : Option[String] = {
  		val pattern = """(PLAYER|LOGIN)\|\[[0-9]{7}\].*""".r
  		val args = pattern.findFirstIn(logLine)
  		if (opeId != None && args != None) {
  			val index = opeLookup.get(opeId.get.split('[').last)
  			if (index != None) 
  				Some(args.get.split(']').last.split(',')(index.get.toInt).trim)
  			else 
  				None
  		} else {
  			None
  		}
	}

	def paramsXtract() : Option[String] = {
		val pattern = """(PLAYER|LOGIN)\|\[[0-9]{7}\].*""".r
  		val matchRes = pattern.findFirstIn(logLine)
  		if (matchRes != None) Some(matchRes.get.split(']').last) else None
	}

}