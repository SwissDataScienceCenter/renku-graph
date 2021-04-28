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
import cats.effect.{BracketThrow, IO, Sync}
import cats.syntax.all._
import ch.datascience.db.{DbClient, SessionResource, SqlStatement}
import ch.datascience.db.implicits._
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
import skunk.codec.all._
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private class AwaitingGenerationEventFinderImpl[Interpretation[_]: Sync: BracketThrow](
    sessionResource:       SessionResource[Interpretation, EventLogDB],
    waitingEventsGauge:    LabeledGauge[Interpretation, projects.Path],
    underProcessingGauge:  LabeledGauge[Interpretation, projects.Path],
    queriesExecTimes:      LabeledHistogram[Interpretation, SqlStatement.Name],
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

  private def findEventAndUpdateForProcessing = for {
    maybeProject <- measureExecutionTime(findProjectsWithEventsInQueue)
                      .map(projectPrioritisation.prioritise)
                      .map(selectProject)
    maybeIdAndProjectAndBody <- maybeProject
                                  .map(idAndPath => measureExecutionTime(findOldestEvent(idAndPath)))
                                  .getOrElse(Kleisli.pure(Option.empty[AwaitingGenerationEvent]))
    maybeBody <- markAsProcessing(maybeIdAndProjectAndBody)
  } yield maybeProject -> maybeBody

  private def findProjectsWithEventsInQueue =
    SqlStatement(
      name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find projects")
    ).select[EventStatus ~ ExecutionDate ~ Int, ProjectInfo](
      sql"""
      SELECT
        proj.project_id,
        proj.project_path,
        proj.latest_event_date,
        (SELECT count(event_id) from event evt_int where evt_int.project_id = proj.project_id and evt_int.status = $eventStatusEncoder) as current_occupancy
      FROM (
        SELECT DISTINCT
          proj.project_id,
          proj.project_path,
          proj.latest_event_date
        FROM event evt
        JOIN project proj on evt.project_id = proj.project_id
        WHERE #${`status IN`(New, GenerationRecoverableFailure)} AND execution_date < $executionDateEncoder
        ORDER BY proj.latest_event_date DESC
        LIMIT $int4
      ) proj
      """
        .query(projectIdDecoder ~ projectPathDecoder ~ eventDateDecoder ~ int8)
        .map { case projectId ~ projectPath ~ eventDate ~ (currentOccupancy: Long) =>
          ProjectInfo(projectId, projectPath, eventDate, Refined.unsafeApply(currentOccupancy.toInt))
        }
    ).arguments(GeneratingTriples ~ ExecutionDate(now()) ~ projectsFetchingLimit.value)
      .build(_.toList)

  private def findOldestEvent(idAndPath: ProjectIds) = {
    val executionDate = ExecutionDate(now())
    SqlStatement(
      name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find oldest")
    ).select[projects.Path ~ projects.Id ~ ExecutionDate ~ ExecutionDate, AwaitingGenerationEvent](
      sql"""
       SELECT evt.event_id, evt.project_id, $projectPathEncoder AS project_path, evt.event_body
       FROM (
         SELECT project_id, min(event_date) AS min_event_date
         FROM event
         WHERE project_id = $projectIdEncoder
           AND #${`status IN`(New, GenerationRecoverableFailure)}
           AND execution_date < $executionDateEncoder
         GROUP BY project_id
       ) oldest_event_date
       JOIN event evt ON oldest_event_date.project_id = evt.project_id 
         AND oldest_event_date.min_event_date = evt.event_date
         AND #${`status IN`(New, GenerationRecoverableFailure)}
         AND execution_date < $executionDateEncoder
       LIMIT 1
       """
        .query(awaitingGenerationEventGet)
    ).arguments(idAndPath.path ~ idAndPath.id ~ executionDate ~ executionDate)
      .build(_.option)
  }

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
      measureExecutionTime(updateStatus(id)) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateStatus(commitEventId: CompoundEventId) = SqlStatement[Interpretation](name =
    Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - update status")
  ).command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus](
    sql"""
        UPDATE event 
        SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
        WHERE event_id = $eventIdEncoder
          AND project_id = $projectIdEncoder
          AND status <> $eventStatusEncoder
        """.command
  ).arguments(
    GeneratingTriples ~ ExecutionDate(now()) ~ commitEventId.id ~ commitEventId.projectId ~ GeneratingTriples
  ).build

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
    (compoundEventIdDecoder ~ projectPathDecoder ~ eventBodyDecoder).gmap[AwaitingGenerationEvent]
}

private object IOAwaitingGenerationEventFinder {

  private val ProjectsFetchingLimit: Int Refined Positive = 10

  def apply(
      sessionResource:      SessionResource[IO, EventLogDB],
      subscribers:          Subscribers[IO],
      waitingEventsGauge:   LabeledGauge[IO, projects.Path],
      underProcessingGauge: LabeledGauge[IO, projects.Path],
      queriesExecTimes:     LabeledHistogram[IO, SqlStatement.Name]
  ): IO[EventFinder[IO, AwaitingGenerationEvent]] = for {
    projectPrioritisation <- ProjectPrioritisation(subscribers)
  } yield new AwaitingGenerationEventFinderImpl(sessionResource,
                                                waitingEventsGauge,
                                                underProcessingGauge,
                                                queriesExecTimes,
                                                projectsFetchingLimit = ProjectsFetchingLimit,
                                                projectPrioritisation = projectPrioritisation
  )
}
