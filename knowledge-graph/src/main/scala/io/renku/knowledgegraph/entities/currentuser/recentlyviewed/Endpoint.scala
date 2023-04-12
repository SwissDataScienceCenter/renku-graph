package io.renku.knowledgegraph.entities.currentuser.recentlyviewed

import cats.effect.kernel.Async
import cats.syntax.all._
import io.renku.config.renku
import io.renku.entities.search.RecentEnitiesFinder
import io.renku.graph.model.{GitLabApiUrl, RenkuUrl}
import io.renku.http.server.security.model.AuthUser
import io.renku.knowledgegraph.entities.currentuser.recentlyviewed.Endpoint.EntityType
import org.http4s.Response
import org.http4s.circe.CirceEntityCodec._
import io.renku.knowledgegraph.entities.ModelEncoders._
import org.http4s.dsl.Http4sDsl

trait Endpoint[F[_]] {
  def getRecentlyViewedEntities(user: AuthUser, limit: Int, entityTypes: Set[EntityType]): F[Response[F]]
}

object Endpoint {
  sealed trait EntityType
  object EntityType {
    case object Project extends EntityType
    case object Dataset extends EntityType
  }

  private class Impl[F[_]: Async](finder: RecentEnitiesFinder[F])(implicit
      renkuApiUrl:  renku.ApiUrl,
      gitLabApiUrl: GitLabApiUrl
  ) extends Endpoint[F]
      with Http4sDsl[F] {
    override def getRecentlyViewedEntities(user: AuthUser, limit: Int, entityTypes: Set[EntityType]): F[Response[F]] =
      finder
        .findRecentlyViewedEntities(limit)
        .flatMap(r => Ok(r.results))
  }
}
