package io.renku.projectauth.sparql

import io.renku.triplesstore.{SparqlQuery => SQuery}
import org.http4s.EntityEncoder
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._

sealed trait SparqlUpdate {

  def render: String

}

object SparqlUpdate {
  def raw(sparql: String): SparqlUpdate =
    new SparqlUpdate {
      override def render: String = sparql
    }

  def apply(q: SQuery): SparqlUpdate =
    raw(q.toString)

  implicit def entityEncoder[F[_]]: EntityEncoder[F, SparqlUpdate] =
    EntityEncoder
      .stringEncoder[F]
      .contramap[SparqlUpdate](_.render)
      .withContentType(`Content-Type`(mediaType"application/sparql-update"))
}
