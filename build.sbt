lazy val root = (project in file(".")).
  settings(
    name := "cluster",
    organization := "com.sasaki"
  )

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.14.0"