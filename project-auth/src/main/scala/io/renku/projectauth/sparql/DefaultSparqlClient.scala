package io.renku.projectauth.sparql

import cats.effect.kernel.Concurrent
import cats.effect.{Async, Resource}
import cats.syntax.all._
import fs2.Stream
import fs2.io.net.Network
import io.circe.Json
import io.renku.projectauth.sparql.DefaultSparqlClient.{ConnectionError, SparqlRequestError}
import io.renku.jsonld.JsonLD
import io.renku.projectauth.sparql.ConnectionConfig.RetryConfig
import org.http4s.{BasicCredentials, EntityDecoder, MediaType, Request, Response, Status}
import org.http4s.Method.POST
import org.http4s.implicits._
import org.http4s.client.{Client, ConnectionFailure}
import org.http4s.client.dsl.Http4sClientDsl
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.circe.CirceEntityCodec._
import org.http4s.ember.core.EmberException
import org.http4s.headers.{Accept, Authorization, `Content-Type`}
import org.typelevel.log4cats.Logger

import java.io.IOException
import java.net.{ConnectException, SocketException, UnknownHostException}
import java.nio.channels.ClosedChannelException
import scala.concurrent.duration._

final class DefaultSparqlClient[F[_]: Async: Logger](client: Client[F], config: ConnectionConfig)
    extends SparqlClient[F]
    with Http4sClientDsl[F] {

  private[this] val sparqlResultsJson: MediaType = mediaType"application/sparql-results+json"

  override def update(request: SparqlUpdate): F[Unit] =
    config.retry.fold(update0(request))(retryConnectionErrors(update0(request)))

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
    config.retry.fold(upload0(data))(retryConnectionErrors(upload0(data)))

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
    config.retry.fold(query0(request))(retryConnectionErrors(query(request)))

  private def query0(request: SparqlQuery): F[Json] = {
    val req =
      POST(config.baseUrl / "query")
        .addHeader(Accept(sparqlResultsJson))
        .withBasicAuth(config.basicAuth)
        .withEntity(request)

    client.run(req).use(_.as[Json])
  }

  // TODO this can be moved to some generic utility
  private def retryConnectionErrors[A](fa: F[A])(cfg: RetryConfig): F[A] = {
    val waits = Stream.awakeDelay(cfg.interval).void

    val tries =
      (Stream.eval(fa.attempt) ++
        Stream
          .repeatEval(fa.attempt)
          .zip(waits)
          .map(_._1)).zipWithIndex.take(cfg.maxRetries)

    val result =
      tries
        .flatMap {
          case (Right(v), _) => Stream.emit(v.some)
          case (Left(ConnectionError(ex)), currentTry) =>
            Stream
              .eval(Logger[F].info(s"Request failed with ${ex.getMessage}, trying again $currentTry/${cfg.maxRetries}"))
              .as(None)

          case (Left(ex), _) =>
            Stream.raiseError(ex)
        }
        .takeThrough(_.isEmpty)
        .compile
        .lastOrError

    result.flatMap {
      case Some(v) => v.pure[F]
      case None    => Async[F].raiseError(new Exception(s"Request failed after retrying: $cfg"))
    }
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

  def apply[F[_]: Async: Network: Logger](
      connectionConfig: ConnectionConfig,
      timeout:          Duration = 20.minutes
  ): Resource[F, SparqlClient[F]] =
    EmberClientBuilder
      .default[F]
      .withTimeout(timeout)
      .build
      .map(c => new DefaultSparqlClient[F](c, connectionConfig))

  private object ConnectionError {
    def unapply(ex: Throwable): Option[Throwable] =
      ex match {
        case _: ConnectionFailure | _: ConnectException | _: SocketException | _: UnknownHostException =>
          Some(ex)
        case _: IOException
            if ex.getMessage.toLowerCase
              .contains("connection reset") || ex.getMessage.toLowerCase.contains("broken pipe") =>
          Some(ex)
        case _: EmberException.ReachedEndOfStream => Some(ex)
        case _: ClosedChannelException            => Some(ex)
        case _ => None
      }
  }
}
