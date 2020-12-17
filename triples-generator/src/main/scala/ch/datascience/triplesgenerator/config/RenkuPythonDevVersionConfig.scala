package ch.datascience.triplesgenerator.config

import cats.MonadError
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader

final case class RenkuPythonDevVersion(version: String) extends Product with Serializable

object RenkuPythonDevVersionConfig {

  import ch.datascience.config.ConfigLoader._

  implicit val reader: ConfigReader[Option[RenkuPythonDevVersion]] = ConfigReader[Option[String]].map {
    case Some(version) if version.trim.isEmpty => None
    case Some(version)                         => Some(RenkuPythonDevVersion(version.trim))
    case None                                  => None
  }

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[Option[RenkuPythonDevVersion]] =
    find[Interpretation, Option[RenkuPythonDevVersion]]("renku-python-dev-version", config)
}
