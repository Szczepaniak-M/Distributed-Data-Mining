ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "MLlibExample",
    idePackagePrefix := Some("de.tum.ddm")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0", //  % "provided",  // comment provided for local execution
  "org.apache.spark" %% "spark-mllib" % "3.5.0" // % "provided"    // comment provided for local execution
)

javacOptions ++= Seq("-source", "11", "-target", "11")