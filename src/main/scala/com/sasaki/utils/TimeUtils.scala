package com.sasaki.utils

import java.text.SimpleDateFormat

object TimeUtils {

	def toMillis(timestamp : String) = {
  		val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  		sdf.parse(timestamp).getTime
  	}

}