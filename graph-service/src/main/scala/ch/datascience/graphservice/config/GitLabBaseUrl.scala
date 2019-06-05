package ch.datascience.graphservice.config

import cats.MonadError
import cats.implicits._
import ch.datascience.config.ConfigLoader.find
import ch.datascience.graphservice.rdfstore.RDFStoreConfig.FusekiBaseUrl
import ch.datascience.tinytypes.constraints.{Url, UrlOps}
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import com.typesafe.config.{Config, ConfigFactory}
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

import scala.language.higherKinds

class GitLabBaseUrl private (val value: String) extends AnyVal with TinyType[String]
object GitLabBaseUrl
    extends TinyTypeFactory[String, GitLabBaseUrl](new GitLabBaseUrl(_))
    with Url
    with UrlOps[GitLabBaseUrl] {

  def apply[Interpretation[_]](
      config:    Config = ConfigFactory.load()
  )(implicit ME: MonadError[Interpretation, Throwable]): Interpretation[GitLabBaseUrl] =
    find[Interpretation, GitLabBaseUrl]("services.gitlab.url", config)

  private implicit val gitLabBaseUrlReader: ConfigReader[GitLabBaseUrl] =
    ConfigReader.fromString[GitLabBaseUrl] { value =>
      GitLabBaseUrl
        .from(value)
        .leftMap(exception => CannotConvert(value, FusekiBaseUrl.getClass.toString, exception.getMessage))
    }
}
