package io.renku.triplesstore.client.http

import cats.MonadThrow
import cats.effect.{Async, Resource}
import fs2.io.net.Network
import io.circe.{Decoder, Json}
import io.renku.jsonld.JsonLD
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.Logger

import scala.concurrent.duration._

trait SparqlClient[F[_]] {

  /** The sparql update operation. */
  def update(request: SparqlUpdate): F[Unit]

  /** Upload rdf data. Not an official sparql operation, but Jena supports it.  */
  def upload(data: JsonLD): F[Unit]

  /** The sparql query operation, returning results as JSON. */
  def query(request: SparqlQuery): F[Json]

  def queryDecode[A](request: SparqlQuery)(implicit d: RowDecoder[A], F: MonadThrow[F]): F[List[A]] = {
    val decoder = Decoder.instance(c => c.downField("results").downField("bindings").as[List[A]])
    F.flatMap(query(request))(json => decoder.decodeJson(json).fold(F.raiseError, F.pure))
  }
}

object SparqlClient {
  def apply[F[_]: Async: Network: Logger](
      connectionConfig: ConnectionConfig,
      timeout:          Duration = 20.minutes
  ): Resource[F, SparqlClient[F]] =
    EmberClientBuilder
      .default[F]
      .withTimeout(timeout)
      .build
      .map(c => new DefaultSparqlClient[F](c, connectionConfig))
}
