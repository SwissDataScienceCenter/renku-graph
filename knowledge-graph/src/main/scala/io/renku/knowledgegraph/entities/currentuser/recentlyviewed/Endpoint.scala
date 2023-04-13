package io.renku.knowledgegraph.entities.currentuser.recentlyviewed

import cats.NonEmptyParallel
import cats.effect.kernel.Async
import cats.syntax.all._
import io.renku.config.renku
import io.renku.entities.viewings.search.RecentEntitiesFinder
import io.renku.graph.config.GitLabUrlLoader
import io.renku.graph.model.GitLabUrl
import io.renku.knowledgegraph.entities.ModelEncoders._
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder}
import org.http4s.Response
import org.http4s.circe.CirceEntityCodec._
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

trait Endpoint[F[_]] {
  def getRecentlyViewedEntities(criteria: RecentEntitiesFinder.Criteria): F[Response[F]]
}

object Endpoint {

  def apply[F[_]: Async: NonEmptyParallel: Logger: SparqlQueryTimeRecorder](
      connectionConfig: ProjectsConnectionConfig
  ): F[Endpoint[F]] = for {
    entitiesFinder                       <- RecentEntitiesFinder[F](connectionConfig)
    implicit0(renkuApiUrl: renku.ApiUrl) <- renku.ApiUrl()
    implicit0(gitLabUrl: GitLabUrl)      <- GitLabUrlLoader[F]()
  } yield new Impl[F](entitiesFinder)

  private class Impl[F[_]: Async](finder: RecentEntitiesFinder[F])(implicit
      renkuApiUrl:  renku.ApiUrl,
      gitLabApiUrl: GitLabUrl
  ) extends Endpoint[F]
      with Http4sDsl[F] {

    override def getRecentlyViewedEntities(criteria: RecentEntitiesFinder.Criteria): F[Response[F]] =
      finder
        .findRecentlyViewedEntities(criteria)
        .flatMap(r => Ok(r.results))
  }
}
