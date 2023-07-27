package io.renku.triplesstore.client.http

import cats.effect._
import cats.syntax.all._
import io.circe.Json
import io.renku.jsonld.JsonLD
import io.renku.triplesstore.client.http.DefaultSparqlClient.SparqlRequestError
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec._
import org.http4s.client.Client
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.headers.{Accept, Authorization, `Content-Type`}
import org.http4s.implicits._
import org.http4s.{BasicCredentials, EntityDecoder, MediaType, Request, Response, Status}
import org.typelevel.log4cats.Logger

final class DefaultSparqlClient[F[_]: Async: Logger](client: Client[F], config: ConnectionConfig)
    extends SparqlClient[F]
    with Http4sClientDsl[F] {

  private[this] val retry = config.retry.map(c => Retry[F](c.interval, c.maxRetries))
  private[this] val sparqlResultsJson: MediaType = mediaType"application/sparql-results+json"

  override def update(request: SparqlUpdate): F[Unit] =
    retry.fold(update0(request))(_.retryConnectionError(update0(request)))

  private def update0(request: SparqlUpdate): F[Unit] = {
    val req =
      POST(config.baseUrl / "update")
        .putHeaders(Accept(sparqlResultsJson))
        .withBasicAuth(config.basicAuth)
        .withEntity(request)

    client.run(req).use { resp =>
      if (resp.status.isSuccess) ().pure[F]
      else SparqlRequestError(resp).flatMap(Async[F].raiseError)
    }
  }

  override def upload(data: JsonLD): F[Unit] =
    retry.fold(upload0(data))(_.retryConnectionError(upload0(data)))

  private def upload0(data: JsonLD): F[Unit] = {
    val req =
      POST(config.baseUrl / "data")
        .putHeaders(Accept(sparqlResultsJson))
        .withBasicAuth(config.basicAuth)
        .withEntity(data.toJson)
        .withContentType(`Content-Type`(MediaType.application.`ld+json`))

    client.run(req).use { resp =>
      if (resp.status.isSuccess) ().pure[F]
      else SparqlRequestError(resp).flatMap(Async[F].raiseError)
    }
  }

  override def query(request: SparqlQuery): F[Json] =
    retry.fold(query0(request))(_.retryConnectionError(query(request)))

  private def query0(request: SparqlQuery): F[Json] = {
    val req =
      POST(config.baseUrl / "query")
        .addHeader(Accept(sparqlResultsJson))
        .withBasicAuth(config.basicAuth)
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
}
