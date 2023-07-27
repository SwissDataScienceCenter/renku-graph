package io.renku.triplesstore.client.http

import io.renku.triplesstore.client.sparql.Fragment
import org.http4s.EntityEncoder
import org.http4s.headers.`Content-Type`
import org.http4s.implicits._

trait SparqlUpdate {
  def render: String
}

object SparqlUpdate {
  final case class Raw(render: String) extends SparqlUpdate

  def raw(sparql: String): SparqlUpdate = Raw(sparql)

  def apply(fr: Fragment): SparqlUpdate = raw(fr.sparql)

  implicit def entityEncoder[F[_]]: EntityEncoder[F, SparqlUpdate] =
    EntityEncoder
      .stringEncoder[F]
      .contramap[SparqlUpdate](_.render)
      .withContentType(`Content-Type`(mediaType"application/sparql-update"))
}
