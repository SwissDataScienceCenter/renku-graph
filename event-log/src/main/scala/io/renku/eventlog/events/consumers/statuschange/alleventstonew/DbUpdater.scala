/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.renku.eventlog.events.consumers.statuschange
package alleventstonew

import cats.Applicative
import cats.data.Kleisli
import cats.effect.Async
import cats.syntax.all._
import io.circe.Encoder
import io.circe.literal._
import io.circe.syntax._
import io.renku.db.{DbClient, SqlStatement}
import io.renku.eventlog.TypeSerializers
import io.renku.eventlog.api.events.StatusChangeEvent.{AllEventsToNew, ProjectEventsToNew}
import io.renku.eventlog.events.consumers.statuschange
import io.renku.eventlog.events.consumers.statuschange.DBUpdater.{RollbackOp, UpdateOp}
import io.renku.eventlog.metrics.QueriesExecutionTimes
import io.renku.events.consumers.Project
import io.renku.events.producers.EventSender
import io.renku.events.{CategoryName, EventRequestContent}
import io.renku.graph.config.EventLogUrl
import io.renku.graph.model.projects
import io.renku.metrics.MetricsRegistry
import org.typelevel.log4cats.Logger
import skunk._
import skunk.implicits._

private[statuschange] class DbUpdater[F[_]: Async: QueriesExecutionTimes](
    eventSender: EventSender[F]
) extends DbClient(Some(QueriesExecutionTimes[F]))
    with statuschange.DBUpdater[F, AllEventsToNew.type]
    with TypeSerializers {

  override def updateDB(event: AllEventsToNew.type): UpdateOp[F] =
    createEventsResource(sendEventIfFound(_))
      .as(DBUpdateResults.ForProjects.empty)

  override def onRollback(event: AllEventsToNew.type): RollbackOp[F] = RollbackOp.empty[F]

  private def createEventsResource(
      f: Cursor[F, ProjectEventsToNew] => F[Unit]
  ): Kleisli[F, Session[F], Unit] = measureExecutionTime {
    SqlStatement
      .named("all_to_new - find projects")
      .select[Void, ProjectEventsToNew](
        sql"""SELECT proj.project_id, proj.project_slug
              FROM project proj
              ORDER BY proj.latest_event_date ASC"""
          .query(projectIdDecoder ~ projectSlugDecoder)
          .map { case (id: projects.GitLabId) ~ (slug: projects.Slug) => ProjectEventsToNew(Project(id, slug)) }
      )
      .arguments(Void)
      .buildCursorResource(f)
  }

  private def sendEventIfFound(cursor: Cursor[F, ProjectEventsToNew], checkForMore: Boolean = true): F[Unit] =
    Applicative[F].whenA(checkForMore) {
      cursor.fetch(1) >>= {
        case (Nil, _) => ().pure[F]
        case (event :: _, areMore) =>
          eventSender.sendEvent(
            EventRequestContent.NoPayload(event.asJson),
            EventSender.EventContext(
              CategoryName(projecteventstonew.eventType.show),
              show"$categoryName: generating ${projecteventstonew.eventType} for ${event.project} failed"
            )
          ) >> sendEventIfFound(cursor, areMore)
      }
    }

  private implicit val encoder: Encoder[ProjectEventsToNew] = Encoder.instance { event =>
    json"""{
      "categoryName": $categoryName,
      "project": {
        "id":   ${event.project.id},
        "slug": ${event.project.slug}
      },
      "subCategory": "ProjectEventsToNew"
    }"""
  }
}

private[statuschange] object DbUpdater {
  def apply[F[_]: Async: Logger: MetricsRegistry: QueriesExecutionTimes]: F[DbUpdater[F]] =
    EventSender[F](EventLogUrl).map(new DbUpdater(_))
}
