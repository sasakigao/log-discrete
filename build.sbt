lazy val root = (project in file(".")).
  settings(
    name := "discrete",
    organization := "com.sasaki",
    version := "1.0"
  )

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.14.0"