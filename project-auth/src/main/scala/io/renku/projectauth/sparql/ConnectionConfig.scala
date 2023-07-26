package io.renku.projectauth.sparql

import org.http4s.{BasicCredentials, Uri}

final case class ConnectionConfig(
    baseUrl:   Uri,
    basicAuth: Option[BasicCredentials]
)
