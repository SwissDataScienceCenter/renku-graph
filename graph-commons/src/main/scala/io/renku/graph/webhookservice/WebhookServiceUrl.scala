package io.renku.graph.webhookservice

import cats.MonadThrow
import com.typesafe.config.{Config, ConfigFactory}
import io.renku.config.ConfigLoader.{find, urlTinyTypeReader}
import io.renku.tinytypes.constraints.{Url, UrlOps}
import io.renku.tinytypes.{TinyTypeFactory, UrlTinyType}
import pureconfig.ConfigReader

final class WebhookServiceUrl private (val value: String) extends AnyVal with UrlTinyType
object WebhookServiceUrl
    extends TinyTypeFactory[WebhookServiceUrl](new WebhookServiceUrl(_))
    with Url
    with UrlOps[WebhookServiceUrl] {

  private implicit val configReader: ConfigReader[WebhookServiceUrl] = urlTinyTypeReader(WebhookServiceUrl)

  def apply[F[_]: MonadThrow](
      config: Config = ConfigFactory.load
  ): F[WebhookServiceUrl] =
    find[F, WebhookServiceUrl]("services.webhook-service.url", config)

}
