/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions.awaitinggeneration

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{Async, Bracket, ContextShift, IO}
import cats.free.Free
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlQuery}
import ch.datascience.graph.model.events.EventStatus._
import ch.datascience.graph.model.events.{CompoundEventId, EventId, EventStatus}
import ch.datascience.graph.model.projects
import ch.datascience.metrics.{LabeledGauge, LabeledHistogram}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.awaitinggeneration.ProjectPrioritisation.{Priority, ProjectInfo}
import io.renku.eventlog.subscriptions.{EventFinder, ProjectIds, Subscribers, SubscriptionTypeSerializers}
import skunk._
import skunk.implicits._
import skunk.codec.all._
import skunk.data.Completion

import java.time.Instant
import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private class AwaitingGenerationEventFinderImpl[Interpretation[_]: Async: Bracket[*[_], Throwable]: ContextShift](
    sessionResource:       SessionResource[Interpretation, EventLogDB],
    waitingEventsGauge:    LabeledGauge[Interpretation, projects.Path],
    underProcessingGauge:  LabeledGauge[Interpretation, projects.Path],
    queriesExecTimes:      LabeledHistogram[Interpretation, SqlQuery.Name],
    now:                   () => Instant = () => Instant.now,
    projectsFetchingLimit: Int Refined Positive,
    projectPrioritisation: ProjectPrioritisation[Interpretation],
    pickRandomlyFrom:      List[ProjectIds] => Option[ProjectIds] = ids => ids.get(Random nextInt ids.size)
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[Interpretation, AwaitingGenerationEvent]
    with SubscriptionTypeSerializers
    with TypeSerializers {

  override def popEvent(): Interpretation[Option[AwaitingGenerationEvent]] = sessionResource.useK {
    for {
      maybeProjectAwaitingGenerationEvent <- findEventAndUpdateForProcessing
      (maybeProject, maybeAwaitingGenerationEvent) = maybeProjectAwaitingGenerationEvent
      _ <- maybeUpdateMetrics(maybeProject, maybeAwaitingGenerationEvent)
    } yield maybeAwaitingGenerationEvent

  }

  private lazy val findEventAndUpdateForProcessing = for {
    maybeProject <- measureExecutionTimeK(findProjectsWithEventsInQueue)
                      .map(projectPrioritisation.prioritise)
                      .map(selectProject)
    maybeIdAndProjectAndBody <- maybeProject
                                  .map(idAndPath => measureExecutionTimeK(findOldestEvent(idAndPath)))
                                  .getOrElse(Kleisli.pure(Option.empty[AwaitingGenerationEvent]))
    maybeBody <- markAsProcessing(maybeIdAndProjectAndBody)
  } yield maybeProject -> maybeBody

  // format: off
  private def findProjectsWithEventsInQueue =  
    SqlQuery(Kleisli[Interpretation, Session[Interpretation], List[ProjectInfo]] {
      session =>
        val query: Query[EventStatus ~ ExecutionDate ~ Int, ProjectInfo] =
          sql"""
      SELECT
        proj.project_id,
        proj.project_path,
        proj.latest_event_date,
        (SELECT count(event_id) from event evt_int where evt_int.project_id = proj.project_id and evt_int.status = $eventStatusPut) as current_occupancy
      FROM (
        SELECT DISTINCT
          proj.project_id,
          proj.project_path,
          proj.latest_event_date
        FROM event evt
        JOIN project proj on evt.project_id = proj.project_id
        WHERE #${`status IN`(New, GenerationRecoverableFailure)} AND execution_date < $executionDatePut
        ORDER BY proj.latest_event_date DESC
        LIMIT $int4
      ) proj
      """.query(projectIdGet ~ projectPathGet ~ eventDateGet ~ int8)
            .map { case projectId ~ projectPath ~ eventDate ~ (currentOccupancy: Long) => ProjectInfo(projectId, projectPath, eventDate, Refined.unsafeApply(currentOccupancy.toInt)) }
        session.prepare(query).use {
          _.stream(GeneratingTriples ~ ExecutionDate(now()) ~ projectsFetchingLimit.value, 32).compile.toList
        }
    },
      name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find projects")
    )
  // format: on

  // format: off
  private def findOldestEvent(idAndPath: ProjectIds) = SqlQuery[Interpretation, Option[AwaitingGenerationEvent]](Kleisli { session =>
    val query: Query[projects.Path ~ projects.Id ~ ExecutionDate ~ ExecutionDate, AwaitingGenerationEvent] =
      sql"""
       SELECT evt.event_id, evt.project_id, $projectPathPut AS project_path, evt.event_body
       FROM (
         SELECT project_id, min(event_date) AS min_event_date
         FROM event
         WHERE project_id = $projectIdPut
           AND #${`status IN`(New, GenerationRecoverableFailure)}
           AND execution_date < $executionDatePut
         GROUP BY project_id
       ) oldest_event_date
       JOIN event evt ON oldest_event_date.project_id = evt.project_id 
         AND oldest_event_date.min_event_date = evt.event_date
         AND #${`status IN`(New, GenerationRecoverableFailure)}
         AND execution_date < $executionDatePut
       LIMIT 1
       """
        .query(awaitingGenerationEventGet)
    val executionDate = ExecutionDate(now())
    session.prepare(query).use(_.option(idAndPath.path ~ idAndPath.id ~ executionDate ~ executionDate))

  },
    name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find oldest")
  )
  // format: on

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    s"status IN (${NonEmptyList.of(status, otherStatuses: _*).toList.map(el => s"'$el'").mkString(",")})"

  private lazy val selectProject: List[(ProjectIds, Priority)] => Option[ProjectIds] = {
    case Nil                          => None
    case (projectIdAndPath, _) +: Nil => Some(projectIdAndPath)
    case many                         => pickRandomlyFrom(prioritiesList(from = many))
  }

  private def prioritiesList(from: List[(ProjectIds, Priority)]): List[ProjectIds] =
    from.foldLeft(List.empty[ProjectIds]) { case (acc, (projectIdAndPath, priority)) =>
      acc :++ List.fill((priority.value * 10).setScale(2, RoundingMode.HALF_UP).toInt)(projectIdAndPath)
    }

  private lazy val markAsProcessing: Option[AwaitingGenerationEvent] => Kleisli[Interpretation, Session[
    Interpretation
  ], Option[AwaitingGenerationEvent]] = {
    case None =>
      Kleisli.pure(Option.empty[AwaitingGenerationEvent])
    case Some(event @ AwaitingGenerationEvent(id, _, _)) =>
      measureExecutionTimeK(updateStatus(id)) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateStatus(commitEventId: CompoundEventId) = SqlQuery[Interpretation, Completion](
    Kleisli { session =>
      val query: Command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus] =
        sql"""
        UPDATE event 
        SET status = $eventStatusPut, execution_date = $executionDatePut
        WHERE event_id = $eventIdPut
          AND project_id = $projectIdPut
          AND status <> $eventStatusPut
        """.command
      session
        .prepare(query)
        .use(
          _.execute(
            GeneratingTriples ~ ExecutionDate(now()) ~ commitEventId.id ~ commitEventId.projectId ~ GeneratingTriples
          )
        )
    },
    name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - update status")
  )

  private def toNoneIfEventAlreadyTaken(
      event: AwaitingGenerationEvent
  ): Completion => Option[AwaitingGenerationEvent] = {
    case Completion.Update(1) => Some(event)
    case _                    => None
  }

  private def maybeUpdateMetrics(maybeProject: Option[ProjectIds], maybeBody: Option[AwaitingGenerationEvent]) =
    (maybeBody, maybeProject) mapN { case (_, ProjectIds(_, projectPath)) =>
      Kleisli.liftF {
        for {
          _ <- waitingEventsGauge decrement projectPath
          _ <- underProcessingGauge increment projectPath
        } yield ()
      }
    } getOrElse Kleisli.pure[Interpretation, Session[Interpretation], Unit](())

  private val awaitingGenerationEventGet: Decoder[AwaitingGenerationEvent] =
    (compoundEventIdGet ~ projectPathGet ~ eventBodyGet).gmap[AwaitingGenerationEvent]
}

private object IOAwaitingGenerationEventFinder {

  private val ProjectsFetchingLimit: Int Refined Positive = 10

  def apply(
      sessionResource:      SessionResource[IO, EventLogDB],
      subscribers:          Subscribers[IO],
      waitingEventsGauge:   LabeledGauge[IO, projects.Path],
      underProcessingGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:     LabeledHistogram[IO, SqlQuery.Name]
  )(implicit contextShift:  ContextShift[IO]): IO[EventFinder[IO, AwaitingGenerationEvent]] = for {
    projectPrioritisation <- ProjectPrioritisation(subscribers)
  } yield new AwaitingGenerationEventFinderImpl(sessionResource,
                                                waitingEventsGauge,
                                                underProcessingGauge,
                                                queriesExecTimes,
                                                projectsFetchingLimit = ProjectsFetchingLimit,
                                                projectPrioritisation = projectPrioritisation
  )
}
