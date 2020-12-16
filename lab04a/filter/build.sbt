lazy val commonSettings = Seq(
  name := "filter",
  version := "0.1",
  scalaVersion := "2.11.12",
  libraryDependencies ++= Seq(
    "org.apache.spark"    %%  "spark-core"   %  "2.4.7"   %   Provided,
    "org.apache.spark"    %%  "spark-sql"    %  "2.4.7"   %   Provided,
    "org.apache.spark"    %%  "spark-mllib"  %  "2.4.7"   %   Provided
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