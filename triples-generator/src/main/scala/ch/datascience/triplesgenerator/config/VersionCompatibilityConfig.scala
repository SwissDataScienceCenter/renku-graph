package ch.datascience.triplesgenerator.config

import cats.MonadError
import ch.datascience.graph.model.CliVersion
import ch.datascience.graph.model.projects.SchemaVersion
import com.typesafe.config.{Config, ConfigFactory, ConfigList}
import pureconfig.ConfigReader

final case class VersionCompatibilityConfig(matrix: List[(CliVersion, SchemaVersion)]) extends Product with Serializable
final case class VersionSchemaPair(cliVersion: CliVersion, schemaVersion: SchemaVersion)
    extends Product
    with Serializable

object VersionCompatibilityConfig {

  import cats.syntax.all._
  import ch.datascience.config.ConfigLoader._
  import pureconfig.generic.auto._

  implicit val reader = ConfigReader[List[List[String]]].map {
    _.map { s =>
      VersionSchemaPair(CliVersion(s(0)), SchemaVersion(s(1)))
    }
  }

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[VersionCompatibilityConfig] =
    find[Interpretation, List[VersionSchemaPair]]("compatibility-matrix", config)(reader, ME)
      .map { case a =>
        println("Cool"); VersionCompatibilityConfig(List.empty[(CliVersion, SchemaVersion)])
      }
}
