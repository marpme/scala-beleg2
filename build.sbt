name := "Beleg2_LogAnalyse"

version := "0.1"

scalaVersion := "2.11.8"

dependencyOverrides ++= Set(
  "io.netty" % "netty" % "3.9.9.Final",
  "commons-net" % "commons-net" % "2.2",
  "com.google.guava" % "guava" % "11.0.2"
)

libraryDependencies ++=Seq("org.apache.spark" %% "spark-core" % "2.2.0",
			   "org.apache.spark" %% "spark-sql" % "2.2.0",
			   "org.jfree" % "jfreechart" % "1.0.19",
				"org.scalactic" %% "scalactic" % "3.0.1",
			"org.scalatest" %% "scalatest" % "3.0.1" % "test",
			   "junit" % "junit" % "4.12" % "test")

