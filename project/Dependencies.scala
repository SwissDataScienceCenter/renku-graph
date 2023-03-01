import sbt._

//noinspection TypeAnnotation
object Dependencies {

  object V {
    val rdf4jQueryParserSparql = "4.2.3"
    val ammonite               = "2.4.1"
    val diffx                  = "0.8.2"
    val http4s                 = "0.23.18"
    val http4sBlaze            = "0.23.13"
    val http4sPrometheus       = "0.24.3"
    val logback                = "1.4.5"
    val log4cats               = "2.5.0"
    val log4jCore              = "2.20.0"
    val luceneQueryParser      = "9.5.0"
    val refined                = "0.10.1"
    val refinedPureconfig      = "0.10.1"
    val circeCore              = "0.14.4"
    val circeOptics            = "0.14.1"
    val jsonld4s               = "0.9.0"
    val catsCore               = "2.9.0"
    val catsEffect             = "3.4.8"
    val monocle                = "2.1.0"
    val scalacheck             = "1.17.0"
    val scalatest              = "3.2.15"
    val scalatestScalacheck    = "3.2.2.0"
    val scalamock              = "5.2.0"
    val sentryLogback          = "6.14.0"
    val owlapi                 = "5.5.0"
    val pureconfig             = "0.17.2"
    val skunk                  = "0.5.1"
    val swaggerParser          = "2.1.12"
    val testContainersScala    = "0.40.12"
    val wiremock               = "2.35.0"
    val widoco                 = "1.4.17"
  }

  val scalatestScalaCheck = Seq(
    "org.scalatestplus" %% "scalacheck-1-14" % V.scalatestScalacheck
  )

  val scalatest = Seq(
    "org.scalatest" %% "scalatest" % V.scalatest
  )

  val ammoniteOps = Seq(
    "com.lihaoyi" %% "ammonite-ops" % V.ammonite
  )

  val rdf4jQueryParserSparql = Seq(
    "org.eclipse.rdf4j" % "rdf4j-queryparser-sparql" % V.rdf4jQueryParserSparql
  )

  val monocle = Seq(
    // libraryDependencies += "dev.optics" %% "monocle-core" % 3.x.x // to be used when circe-optics starts to use is
    "com.github.julien-truffaut" %% "monocle-core" % V.monocle
  )

  val diffx = Seq(
    "com.softwaremill.diffx" %% "diffx-scalatest-should" % V.diffx
  )

  val swaggerParser = Seq(
    "io.swagger.parser.v3" % "swagger-parser" % V.swaggerParser
  )

  val widoco = Seq(
    "com.github.dgarijo" % "widoco" % V.widoco,
    // needed by widoco only - explicitly bumped up to work with rdf4j-queryparser-sparql (triples-store-client)
    // from 5.1.18 that widoco comes with
    "net.sourceforge.owlapi" % "owlapi-distribution" % V.owlapi,
    // needed by widoco only
    "org.apache.logging.log4j" % "log4j-core" % V.log4jCore
  )

  val pureconfig = Seq(
    "com.github.pureconfig" %% "pureconfig"      % V.pureconfig,
    "com.github.pureconfig" %% "pureconfig-cats" % V.pureconfig
  )

  val refinedPureconfig = Seq(
    "eu.timepit" %% "refined-pureconfig" % V.refinedPureconfig
  )

  val sentryLogback = Seq(
    "io.sentry" % "sentry-logback" % V.sentryLogback
  )

  val luceneQueryParser = Seq(
    "org.apache.lucene" % "lucene-queryparser" % V.luceneQueryParser
  )

  val http4sClient = Seq(
    "org.http4s" %% "http4s-blaze-client" % V.http4sBlaze
  )
  val http4sServer = Seq(
    "org.http4s" %% "http4s-blaze-server" % V.http4sBlaze,
    "org.http4s" %% "http4s-server"       % V.http4s
  )
  val http4sCirce = Seq(
    "org.http4s" %% "http4s-circe" % V.http4s
  )
  val http4sDsl = Seq(
    "org.http4s" %% "http4s-dsl" % V.http4s
  )
  val http4sPrometheus = Seq(
    "org.http4s" %% "http4s-prometheus-metrics" % V.http4sPrometheus
  )

  val skunk = Seq(
    "org.tpolecat" %% "skunk-core" % V.skunk
  )

  val catsEffect = Seq(
    "org.typelevel" %% "cats-effect" % V.catsEffect
  )

  val log4Cats = Seq(
    "org.typelevel" %% "log4cats-core" % V.log4cats
  )

  val testContainersPostgres = Seq(
    "com.dimafeng" %% "testcontainers-scala-scalatest"  % V.testContainersScala,
    "com.dimafeng" %% "testcontainers-scala-postgresql" % V.testContainersScala
  )

  val wiremock = Seq(
    "com.github.tomakehurst" % "wiremock-jre8" % V.wiremock
  )

  val scalamock = Seq(
    "org.scalamock" %% "scalamock" % V.scalamock
  )

  val logbackClassic = Seq(
    "ch.qos.logback" % "logback-classic" % V.logback
  )

  val refined = Seq(
    "eu.timepit" %% "refined" % V.refined
  )

  val circeCore = Seq(
    "io.circe" %% "circe-core" % V.circeCore
  )
  val circeLiteral = Seq(
    "io.circe" %% "circe-literal" % V.circeCore
  )
  val circeGeneric = Seq(
    "io.circe" %% "circe-generic" % V.circeCore
  )
  val circeParser = Seq(
    "io.circe" %% "circe-parser" % V.circeCore
  )

  val circeOptics = Seq(
    "io.circe" %% "circe-optics" % V.circeOptics
  )

  val jsonld4s = Seq(
    "io.renku" %% "jsonld4s" % V.jsonld4s
  )

  val catsCore = Seq(
    "org.typelevel" %% "cats-core" % V.catsCore
  )

  val catsFree = Seq(
    "org.typelevel" %% "cats-free" % V.catsCore
  )

  val scalacheck = Seq(
    "org.scalacheck" %% "scalacheck" % V.scalacheck
  )

}