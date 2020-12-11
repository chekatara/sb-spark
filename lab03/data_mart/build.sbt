lazy val commonSettings = Seq(
  name := "data_mart",
  version := "0.1",
  scalaVersion := "2.11.12",
  libraryDependencies ++= Seq(
    "org.apache.spark" %%  "spark-core" % "2.4.7" % Provided,
    "org.apache.spark" %%  "spark-sql" % "2.4.7" % Provided,
    "org.apache.spark" %%  "spark-mllib" % "2.4.7" % Provided
    //"com.datastax.spark" %% "spark-cassandra-connector" % "2.5.0",
    //"org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.9",
    //"org.postgresql" % "postgresql" % "42.2.12"
  )

)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := s"${name.value}_${version.value}_${scalaVersion.value}.jar"