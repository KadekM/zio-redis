import sbt._
import Keys._
import sbtbuildinfo._
import BuildInfoKeys._
import scalafix.sbt.ScalafixPlugin.autoImport._

object BuildHelper {
  val Scala212 = "2.12.13"
  val Scala213 = "2.13.3"
  val Zio      = "1.0.4"

  def buildInfoSettings(packageName: String) =
    Seq(
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, isSnapshot),
      buildInfoPackage := packageName,
      buildInfoObject := "BuildInfo"
    )

  def stdSettings(prjName: String) =
    Seq(
      name := s"$prjName",
      crossScalaVersions := Seq(Scala212, Scala213),
      ThisBuild / scalaVersion := Scala213,
      ThisBuild / semanticdbEnabled := true,
      ThisBuild / semanticdbOptions += "-P:semanticdb:synthetics:on",
      ThisBuild / semanticdbVersion := scalafixSemanticdb.revision,
      ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value),
      ThisBuild / scalafixDependencies ++= List(
        "com.github.liancheng" %% "organize-imports" % "0.5.0",
        "com.github.vovapolu"  %% "scaluzzi"         % "0.1.17"
      ),
      parallelExecution in Test := true,
      incOptions ~= (_.withLogRecompileOnMacro(false)),
      autoAPIMappings := true
    )
}
