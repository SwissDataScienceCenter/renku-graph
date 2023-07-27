package io.renku.projectauth.sparql

import org.http4s.{BasicCredentials, Uri}

import scala.concurrent.duration.FiniteDuration

final case class ConnectionConfig(
    baseUrl:   Uri,
    basicAuth: Option[BasicCredentials],
    retry:     Option[ConnectionConfig.RetryConfig]
)

object ConnectionConfig {

  final case class RetryConfig(
      interval:   FiniteDuration,
      maxRetries: Int
  )
}
