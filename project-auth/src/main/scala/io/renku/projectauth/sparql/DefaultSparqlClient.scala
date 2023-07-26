package io.renku.projectauth.sparql

import cats.effect.kernel.Concurrent
import cats.effect.{Async, Resource}
import cats.syntax.all._
import fs2.io.net.Network
import io.circe.Json
import io.renku.projectauth.sparql.DefaultSparqlClient.SparqlRequestError
import io.renku.jsonld.JsonLD
import org.http4s.{BasicCredentials, EntityDecoder, MediaType, Request, Response, Status}
import org.http4s.Method.POST
import org.http4s.implicits._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.circe.CirceEntityCodec._
import org.http4s.headers.{Accept, Authorization, `Content-Type`}

import scala.concurrent.duration._

final class DefaultSparqlClient[F[_]: Async](client: Client[F], connectionConfig: ConnectionConfig)
    extends SparqlClient[F]
    with Http4sClientDsl[F] {

  private[this] val sparqlResultsJson: MediaType = mediaType"application/sparql-results+json"

  override def update(request: SparqlUpdate): F[Unit] = {
    val req =
      POST(connectionConfig.baseUrl / "update")
        .putHeaders(Accept(sparqlResultsJson))
        .withBasicAuth(connectionConfig.basicAuth)
        .withEntity(request)

    client.run(req).use { resp =>
      if (resp.status.isSuccess) ().pure[F]
      else SparqlRequestError(resp).flatMap(Async[F].raiseError)
    }
  }

  def upload(data: JsonLD): F[Unit] = {
    val req =
      POST(connectionConfig.baseUrl / "data")
        .putHeaders(Accept(sparqlResultsJson))
        .withBasicAuth(connectionConfig.basicAuth)
        .withEntity(data.toJson)
        .withContentType(`Content-Type`(MediaType.application.`ld+json`))

    client.run(req).use { resp =>
      if (resp.status.isSuccess) ().pure[F]
      else SparqlRequestError(resp).flatMap(Async[F].raiseError)
    }
  }

  override def query(request: SparqlQuery): F[Json] = {
    val req =
      POST(connectionConfig.baseUrl / "query")
        .addHeader(Accept(sparqlResultsJson))
        .withBasicAuth(connectionConfig.basicAuth)
        .withEntity(request)

    client.run(req).use(_.as[Json])
  }

  final implicit class MoreRequestDsl(req: Request[F]) {
    def withBasicAuth(cred: Option[BasicCredentials]): Request[F] =
      cred.map(c => req.putHeaders(Authorization(c))).getOrElse(req)
  }
}

object DefaultSparqlClient {

  final case class SparqlRequestError(status: Status, body: String)
      extends RuntimeException(s"Request failed with status=$status: $body") {
    override def fillInStackTrace(): Throwable = this
  }
  object SparqlRequestError {
    def apply[F[_]: Concurrent](resp: Response[F]): F[SparqlRequestError] =
      EntityDecoder.decodeText(resp).map(str => SparqlRequestError(resp.status, str))
  }

  def apply[F[_]: Async: Network](
      connectionConfig: ConnectionConfig,
      timeout:          Duration = 20.minutes
  ): Resource[F, SparqlClient[F]] =
    EmberClientBuilder
      .default[F]
      .withTimeout(timeout)
      .build
      .map(c => new DefaultSparqlClient[F](c, connectionConfig))
}
