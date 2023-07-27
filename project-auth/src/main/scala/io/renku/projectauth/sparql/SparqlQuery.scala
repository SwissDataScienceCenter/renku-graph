package io.renku.projectauth.sparql

import io.renku.triplesstore.client.sparql.Fragment
import io.renku.triplesstore.{SparqlQuery => SQuery}
import org.http4s.{EntityEncoder, MediaType}
import org.http4s.headers.`Content-Type`

sealed trait SparqlQuery {

  def render: String

}

object SparqlQuery {
  def raw(sparql: String): SparqlQuery =
    new SparqlQuery {
      override def render: String = sparql
    }

  def apply(q: SQuery): SparqlQuery =
    raw(q.toString)

  def apply(fr: Fragment): SparqlQuery =
    raw(fr.sparql)

  implicit def entityEncoder[F[_]]: EntityEncoder[F, SparqlQuery] =
    EntityEncoder
      .stringEncoder[F]
      .contramap[SparqlQuery](_.render)
      .withContentType(`Content-Type`(MediaType.application.`sparql-query`))
}
