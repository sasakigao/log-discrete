lazy val root = (project in file(".")).
  settings(
    name := "discrete",
    organization := "com.sasaki"
  )

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.14.0"