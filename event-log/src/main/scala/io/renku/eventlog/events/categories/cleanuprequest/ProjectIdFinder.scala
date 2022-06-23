package io.renku.eventlog.events.categories.cleanuprequest

import cats.effect.MonadCancelThrow
import cats.syntax.all._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.EventLogDB.SessionResource
import io.renku.eventlog.TypeSerializers
import io.renku.graph.model.projects
import io.renku.metrics.LabeledHistogram

private trait ProjectIdFinder[F[_]] {
  def findProjectId(projectPath: projects.Path): F[Option[projects.Id]]
}

private object ProjectIdFinder {
  def apply[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F]): F[ProjectIdFinder[F]] =
    new ProjectIdFinderImpl[F](queriesExecTimes).pure[F].widen
}

private class ProjectIdFinderImpl[F[_]: MonadCancelThrow: SessionResource](queriesExecTimes: LabeledHistogram[F])
    extends DbClient(Some(queriesExecTimes))
    with ProjectIdFinder[F]
    with TypeSerializers {
  import skunk.implicits._

  override def findProjectId(projectPath: projects.Path): F[Option[projects.Id]] = SessionResource[F].useK {
    measureExecutionTime {
      SqlStatement
        .named(s"${categoryName.show.toLowerCase} - find project_id")
        .select[projects.Path, projects.Id](sql"""
          SELECT project_id
          FROM project
          WHERE project_path = $projectPathEncoder 
        """.query(projectIdDecoder))
        .arguments(projectPath)
        .build(_.option)
    }
  }
}
