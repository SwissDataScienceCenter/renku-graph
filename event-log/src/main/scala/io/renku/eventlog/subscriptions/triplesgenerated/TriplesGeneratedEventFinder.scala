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

package io.renku.eventlog.subscriptions.triplesgenerated

import cats.MonadThrow
import cats.data._
import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.db.implicits._
import io.renku.db.{DbClient, SessionResource, SqlStatement}
import io.renku.eventlog._
import io.renku.eventlog.subscriptions.triplesgenerated.ProjectPrioritisation.{Priority, ProjectInfo}
import io.renku.eventlog.subscriptions.{EventFinder, ProjectIds, SubscriptionTypeSerializers}
import io.renku.graph.model.events.EventStatus._
import io.renku.graph.model.events.{CompoundEventId, EventId, EventStatus}
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, LabeledHistogram}
import skunk._
import skunk.codec.all.{int4, int8}
import skunk.data.Completion
import skunk.implicits._

import java.time.Instant
import scala.math.BigDecimal.RoundingMode
import scala.util.Random

private class TriplesGeneratedEventFinderImpl[F[_]: Async](
    sessionResource:             SessionResource[F, EventLogDB],
    awaitingTransformationGauge: LabeledGauge[F, projects.Path],
    underTransformationGauge:    LabeledGauge[F, projects.Path],
    queriesExecTimes:            LabeledHistogram[F, SqlStatement.Name],
    now:                         () => Instant = () => Instant.now,
    projectsFetchingLimit:       Int Refined Positive,
    projectPrioritisation:       ProjectPrioritisation,
    pickRandomlyFrom: List[ProjectIds] => Option[ProjectIds] = ids => ids.get((Random nextInt ids.size).toLong)
) extends DbClient(Some(queriesExecTimes))
    with EventFinder[F, TriplesGeneratedEvent]
    with SubscriptionTypeSerializers {

  override def popEvent(): F[Option[TriplesGeneratedEvent]] = sessionResource.useK {
    for {
      maybeProjectAndEvent <- findEventAndUpdateForProcessing()
      (maybeProject, maybeTriplesGeneratedEvent) = maybeProjectAndEvent
      _ <- Kleisli.liftF(maybeUpdateMetrics(maybeProject, maybeTriplesGeneratedEvent))
    } yield maybeTriplesGeneratedEvent

  }

  private def findEventAndUpdateForProcessing() = for {
    maybeProject <- measureExecutionTime(findProjectsWithEventsInQueue)
                      .map(projectPrioritisation.prioritise)
                      .map(selectProject)
    maybeIdAndProjectAndBody <- maybeProject
                                  .map(idAndPath => measureExecutionTime(findNewestEvent(idAndPath)))
                                  .getOrElse(Kleisli.pure(Option.empty[TriplesGeneratedEvent]))
    maybeBody <- markAsTransformingTriples(maybeIdAndProjectAndBody)
  } yield maybeProject -> maybeBody

  private def findProjectsWithEventsInQueue = SqlStatement(
    name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find projects")
  ).select[ExecutionDate ~ Int, ProjectInfo](
    sql"""
        SELECT DISTINCT
          proj.project_id,
          proj.project_path,
          proj.latest_event_date,
          (SELECT count(event_id) from event evt_int where evt_int.project_id = proj.project_id and evt_int.status = '#${TransformingTriples.value}') as current_occupancy
        FROM project proj
        WHERE 
          EXISTS (
            SELECT candidate_events.event_id
            FROM (
              SELECT evt.event_id, evt.status
              FROM event evt
              JOIN event_payload evt_payload ON evt.event_id = evt_payload.event_id AND evt.project_id = evt_payload.project_id
              WHERE evt.project_id = proj.project_id 
                AND #${`status IN`(TransformingTriples, TriplesGenerated, TransformationRecoverableFailure)} 
                AND execution_date <= $executionDateEncoder
              ORDER BY evt.event_date DESC
              LIMIT 1
            ) candidate_events
            WHERE candidate_events.status <> '#${TransformingTriples.value}'
          )
        ORDER BY proj.latest_event_date DESC
        LIMIT $int4
      """
      .query(projectIdDecoder ~ projectPathDecoder ~ eventDateDecoder ~ int8)
      .map { case projectId ~ projectPath ~ eventDate ~ (currentOccupancy: Long) =>
        ProjectInfo(projectId, projectPath, eventDate, Refined.unsafeApply(currentOccupancy.toInt))
      }
  ).arguments(ExecutionDate(now()) ~ projectsFetchingLimit.value)
    .build(_.toList)

  private def findNewestEvent(idAndPath: ProjectIds) = {
    val executionDate = ExecutionDate(now())
    SqlStatement(name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - find oldest"))
      .select[projects.Path ~ projects.Id ~ ExecutionDate ~ ExecutionDate, TriplesGeneratedEvent](sql"""
         SELECT evt.event_id, evt.project_id, $projectPathEncoder AS project_path, evt_payload.payload
         FROM (
           SELECT evt_int.project_id, max(event_date) AS max_event_date
           FROM event evt_int
           JOIN event_payload evt_payload ON evt_int.event_id = evt_payload.event_id AND evt_int.project_id = evt_payload.project_id
           WHERE evt_int.project_id = $projectIdEncoder
             AND #${`status IN`(TriplesGenerated, TransformationRecoverableFailure)}
             AND execution_date < $executionDateEncoder
           GROUP BY evt_int.project_id
         ) newest_event_date
         JOIN event evt ON newest_event_date.project_id = evt.project_id 
           AND newest_event_date.max_event_date = evt.event_date
           AND #${`status IN`(TriplesGenerated, TransformationRecoverableFailure)}
           AND execution_date < $executionDateEncoder
         JOIN event_payload evt_payload ON evt.event_id = evt_payload.event_id AND evt.project_id = evt_payload.project_id
         LIMIT 1
         """.query(compoundEventIdDecoder ~ projectPathDecoder ~ zippedPayloadDecoder).map {
        case eventId ~ projectPath ~ eventPayload =>
          TriplesGeneratedEvent(eventId, projectPath, eventPayload)
      })
      .arguments(idAndPath.path ~ idAndPath.id ~ executionDate ~ executionDate)
      .build(_.option)
  }

  private def `status IN`(status: EventStatus, otherStatuses: EventStatus*) =
    s"status IN (${NonEmptyList.of(status, otherStatuses: _*).map(el => s"'$el'").toList.mkString(",")})"

  private lazy val selectProject: List[(ProjectIds, Priority)] => Option[ProjectIds] = {
    case Nil                          => None
    case (projectIdAndPath, _) +: Nil => Some(projectIdAndPath)
    case many                         => pickRandomlyFrom(prioritiesList(from = many))
  }

  private def prioritiesList(from: List[(ProjectIds, Priority)]): List[ProjectIds] =
    from.foldLeft(List.empty[ProjectIds]) { case (acc, (projectIdAndPath, priority)) =>
      acc :++ List.fill((priority.value * 10).setScale(2, RoundingMode.HALF_UP).toInt)(projectIdAndPath)
    }

  private lazy val markAsTransformingTriples: Option[TriplesGeneratedEvent] => Kleisli[F, Session[F], Option[
    TriplesGeneratedEvent
  ]] = {
    case None =>
      Kleisli.pure(Option.empty[TriplesGeneratedEvent])
    case Some(event @ TriplesGeneratedEvent(id, _, _)) =>
      measureExecutionTime(updateStatus(id)) map toNoneIfEventAlreadyTaken(event)
  }

  private def updateStatus(commitEventId: CompoundEventId) =
    SqlStatement(name = Refined.unsafeApply(s"${SubscriptionCategory.name.value.toLowerCase} - update status"))
      .command[EventStatus ~ ExecutionDate ~ EventId ~ projects.Id ~ EventStatus](
        sql"""UPDATE event
              SET status = $eventStatusEncoder, execution_date = $executionDateEncoder
              WHERE event_id = $eventIdEncoder
                AND project_id = $projectIdEncoder
                AND status <> $eventStatusEncoder
        """.command
      )
      .arguments(
        TransformingTriples ~
          ExecutionDate(now()) ~
          commitEventId.id ~
          commitEventId.projectId ~
          TransformingTriples
      )
      .build

  private def toNoneIfEventAlreadyTaken(event: TriplesGeneratedEvent): Completion => Option[TriplesGeneratedEvent] = {
    case Completion.Update(1) => Some(event)
    case _                    => None
  }

  private def maybeUpdateMetrics(maybeProject: Option[ProjectIds], maybeBody: Option[TriplesGeneratedEvent]) =
    (maybeBody, maybeProject) mapN { case (_, ProjectIds(_, projectPath)) =>
      for {
        _ <- awaitingTransformationGauge decrement projectPath
        _ <- underTransformationGauge increment projectPath
      } yield ()
    } getOrElse ().pure[F]
}

private object TriplesGeneratedEventFinder {

  private val ProjectsFetchingLimit: Int Refined Positive = 10

  def apply[F[_]: Async](sessionResource: SessionResource[F, EventLogDB],
                         awaitingTransformationGauge: LabeledGauge[F, projects.Path],
                         underTransformationGauge:    LabeledGauge[F, projects.Path],
                         queriesExecTimes:            LabeledHistogram[F, SqlStatement.Name]
  ): F[EventFinder[F, TriplesGeneratedEvent]] = MonadThrow[F].catchNonFatal {
    new TriplesGeneratedEventFinderImpl(sessionResource,
                                        awaitingTransformationGauge,
                                        underTransformationGauge,
                                        queriesExecTimes,
                                        projectsFetchingLimit = ProjectsFetchingLimit,
                                        projectPrioritisation = new ProjectPrioritisation()
    )
  }
}
