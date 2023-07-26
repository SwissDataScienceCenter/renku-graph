package io.renku.projectauth.sparql

import cats.MonadThrow
import io.circe.{Decoder, Json}
import io.renku.jsonld.JsonLD

trait SparqlClient[F[_]] {

  /** The sparql update operation. */
  def update(request: SparqlUpdate): F[Unit]

  /** Upload rdf data  */
  def upload(data: JsonLD): F[Unit]

  /** The sparql query operation, returning results as JSON. */
  def query(request: SparqlQuery): F[Json]

  def queryDecode[A](request: SparqlQuery)(implicit d: RowDecoder[A], F: MonadThrow[F]): F[List[A]] = {
    val decoder = Decoder.instance(c => c.downField("results").downField("bindings").as[List[A]])
    F.flatMap(query(request))(json => decoder.decodeJson(json).fold(F.raiseError, F.pure))
  }
}
