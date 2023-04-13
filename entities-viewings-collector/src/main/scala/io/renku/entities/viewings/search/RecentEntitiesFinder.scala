package io.renku.entities.viewings.search

import cats.NonEmptyParallel
import cats.effect._
import io.renku.entities.search.model.Entity
import io.renku.http.rest.paging.PagingResponse
import io.renku.http.server.security.model.AuthUser
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

trait RecentEntitiesFinder[F[_]] {

  def findRecentlyViewedEntities(criteria: RecentEntitiesFinder.Criteria): F[PagingResponse[Entity]]

}

object RecentEntitiesFinder {
  sealed trait EntityType
  object EntityType {
    case object Project extends EntityType
    case object Dataset extends EntityType
  }

  final case class Criteria(
      entityTypes: Set[EntityType],
      authUser:    AuthUser,
      limit:       Int
  )

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](
      connConfig: ProjectsConnectionConfig
  ): F[RecentEntitiesFinder[F]] =
    Async[F].pure(new RecentEntitiesFinderImpl[F](connConfig))

}
