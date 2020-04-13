import scalariform.formatter.preferences._

organization := "com.fsat.examples.spark"
name := "api-usage-example"

// Code formatting option
scalariformPreferences in ThisBuild := scalariformPreferences.value
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(AllowParamGroupsOnNewlines, true)

// Library dependencies settings
lazy val Versions = new {
  val scala = "2.11.8"
  val spark = "2.3.0"
  val scopt = "4.0.0-RC2"
  val sparkAvro = "4.0.0"
  val scalaLogging = "3.8.0"
  val logbackClassic = "1.2.3"
  val sparkTestingBase = s"${spark}_0.9.0"
  val scalaTest = "3.0.5"
}

lazy val Libraries = new {
  val sparkCore = "org.apache.spark" %% "spark-core" % Versions.spark % "provided"
  val sparkSql = "org.apache.spark" %% "spark-sql" % Versions.spark % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % Versions.spark % "provided"
  val scopt = "com.github.scopt" %% "scopt" % Versions.scopt
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
  val logbackClassic = "ch.qos.logback" % "logback-classic" % Versions.logbackClassic

  val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % Versions.sparkTestingBase % "test"
  val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % "test"
}

libraryDependencies ++= Seq(
  Libraries.sparkCore,
  Libraries.sparkSql,
  Libraries.sparkHive,
  Libraries.scopt,
  Libraries.scalaLogging,
  Libraries.logbackClassic,
  Libraries.scalaTest,
  Libraries.sparkTestingBase
)


// Scala settings
scalaVersion := Versions.scala
scalacOptions ++= Seq(
  "-deprecation",
  "-unchecked")

// Scala linter settings
def unsafeWarts(except: wartremover.Wart*): Seq[wartremover.Wart] =
  Warts.unsafe.filterNot(except.toSeq.contains)

wartremoverErrors in (Compile, compile) := unsafeWarts(except = Wart.DefaultArguments)
wartremoverErrors in (Test, test) := unsafeWarts(except = Wart.DefaultArguments, Wart.Var, Wart.TryPartial)

// Test settings
fork in Test := true
parallelExecution in Test := false
testForkedParallel in Test := false
logBuffered in Test := false
javaOptions in Test ++= Seq("-Xms512M", "-Xmx3072M", "-XX:MaxPermSize=3072M", "-XX:+CMSClassUnloadingEnabled")
excludeFilter in (Test, unmanagedResources) := new SimpleFileFilter(_ => false)
