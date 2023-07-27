package io.renku.triplesstore.client.http

import io.renku.triplesstore.client.sparql.Fragment
import org.http4s.{EntityEncoder, MediaType}
import org.http4s.headers.`Content-Type`

trait SparqlQuery {
  def render: String
}

object SparqlQuery {
  final case class Raw(render: String) extends SparqlQuery

  def raw(sparql: String): SparqlQuery = Raw(sparql)

  def apply(fr: Fragment): SparqlQuery = raw(fr.sparql)

  implicit def entityEncoder[F[_]]: EntityEncoder[F, SparqlQuery] =
    EntityEncoder
      .stringEncoder[F]
      .contramap[SparqlQuery](_.render)
      .withContentType(`Content-Type`(MediaType.application.`sparql-query`))
}
