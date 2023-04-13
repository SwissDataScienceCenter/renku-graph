package io.renku.entities.viewings.search

import cats.NonEmptyParallel
import cats.effect._
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.entities.search.model.{Entity => SearchEntity}
import io.renku.entities.viewings.search.RecentEntitiesFinder.Criteria
import io.renku.graph.model.Schemas
import io.renku.http.rest.paging.model.{Page, PerPage}
import io.renku.http.rest.paging.{Paging, PagingRequest, PagingResponse}
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQuery, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private class RecentEntitiesFinderImpl[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](
    storeConfig: ProjectsConnectionConfig
) extends RecentEntitiesFinder[F]
    with Paging[SearchEntity] /* why is this modelled using subtyping? */ {
  private[this] val client = TSClient(storeConfig)

  def findRecentlyViewedEntities(criteria: Criteria): F[PagingResponse[SearchEntity]] = {
    implicit val resultsFinder: Paging.PagedResultsFinder[F, SearchEntity] =
      client.pagedResultsFinder[SearchEntity](makeQuery(criteria))

    makeRequest(criteria).flatMap(findPage[F])
  }

  private def makeRequest(c: Criteria) =
    perPageFromCriteria(c).map(pp => PagingRequest(Page.first, pp))

  private def perPageFromCriteria(c: Criteria): F[PerPage] =
    PerPage
      .from(c.limit)
      .fold(Async[F].raiseError(_), _.pure[F])

  def makeQuery(criteria: Criteria): SparqlQuery =
    SparqlQuery.of(
      name = "recent-entity search",
      Prefixes.of(Schemas.prov -> "prov", Schemas.renku -> "renku", Schemas.schema -> "schema", Schemas.xsd -> "xsd"),
      sparql"""|SELECT *
               |WHERE {
               |  ?s ?p ?o
               |  /* $criteria */
               |}
               |ORDER BY ?s
               |""".stripMargin
    )

  implicit def modelDecoder: Decoder[SearchEntity] =
    ???
}
