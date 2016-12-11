name := """spark-front"""
organization := "com.lenielmacaferi"

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.8"

libraryDependencies += filters
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.0-RC1" % Test

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "com.lenielmacaferi.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "com.lenielmacaferi.binders._"

// Compile the project before generating Eclipse files, so that generated .scala or .class files for views and routes are present
EclipseKeys.preTasks := Seq(compile in Compile)