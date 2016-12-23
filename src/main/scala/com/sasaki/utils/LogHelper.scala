package com.sasaki.utils

/**
 * Log the object.
 */
object LogHelper {
	val prefix = "sasaki YO"

	def log[T](content : T, head : String) = {
		println(s"$prefix $head :: $content")
	}

}