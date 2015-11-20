name := "Ignition-Core"

version := "1.0"

scalaVersion := "2.10.4"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings")

ideaExcludeFolders += ".idea"

ideaExcludeFolders += ".idea_modules"

// Because we can't run two spark contexts on same VM
parallelExecution in Test := false

libraryDependencies += ("org.apache.spark" %% "spark-core" % "1.5.1" % "provided")
  .exclude("org.apache.hadoop", "hadoop-client")
  .exclude("org.slf4j", "slf4j-log4j12")

libraryDependencies += ("org.apache.hadoop" % "hadoop-client" % "2.0.0-cdh4.7.1" % "provided")

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.4"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "0.3.16"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.0.6"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.2.0"

libraryDependencies += "net.java.dev.jets3t" % "jets3t" % "0.7.1"

libraryDependencies += "joda-time" % "joda-time" % "2.7"

libraryDependencies += "org.joda" % "joda-convert" % "1.7"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

resolvers += "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers += Resolver.sonatypeRepo("public")
