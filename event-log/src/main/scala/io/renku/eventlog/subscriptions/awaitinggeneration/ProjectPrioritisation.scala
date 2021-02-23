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

import cats.MonadError
import cats.data.NonEmptyList
import cats.syntax.all._
import ch.datascience.graph.model.projects
import ch.datascience.tinytypes.{BigDecimalTinyType, TinyTypeFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.eventlog.EventDate
import io.renku.eventlog.subscriptions.awaitinggeneration.ProjectPrioritisation.{Priority, ProjectInfo}
import io.renku.eventlog.subscriptions.{Capacity, ProjectIds, Subscribers}

import java.time.Duration

private trait ProjectPrioritisation[Interpretation[_]] {
  def prioritise(projects: List[ProjectInfo]): List[(ProjectIds, Priority)]
}

private class ProjectPrioritisationImpl[Interpretation[_]](subscribers: Subscribers[Interpretation])
    extends ProjectPrioritisation[Interpretation] {
  import ProjectPrioritisation.Priority._

  private val FreeSpotsRatio = .15

  override def prioritise(projects: List[ProjectInfo]): List[(ProjectIds, Priority)] =
    (rejectProjectsAboveOccupancyThreshold _ >>>
      findPrioritiesBasedOnLatestEventDate >>>
      correctPrioritiesUsingOccupancyPerProject)(projects)

  private def rejectProjectsAboveOccupancyThreshold(projects: List[ProjectInfo]): List[ProjectInfo] =
    subscribers.getTotalCapacity
      .map(_.value)
      .map { totalCapacity =>
        val freeSpots =
          if ((totalCapacity * FreeSpotsRatio).ceil.intValue() < 2) 2
          else (totalCapacity * FreeSpotsRatio).ceil.intValue()
        projects match {
          case Nil => Nil
          case projects =>
            val totalOccupancy = projects.map(_.currentOccupancy.value).sum
            projects.foldLeft(List.empty[ProjectInfo]) { (acc, project) =>
              if (project.currentOccupancy.value == 0) acc :+ project
              else if (totalOccupancy < totalCapacity - freeSpots) acc :+ project
              else acc
            }
        }
      }
      .getOrElse(projects)

  private lazy val findPrioritiesBasedOnLatestEventDate
      : List[ProjectInfo] => List[(ProjectIds, Priority, Int Refined NonNegative)] = {
    case Nil => Nil
    case ProjectInfo(projectId, projectPath, _, currentOccupancy) :: Nil =>
      List((ProjectIds(projectId, projectPath), MaxPriority, currentOccupancy))
    case projects =>
      val ProjectInfo(_, _, latestEventDate, _) = projects.head
      val ProjectInfo(_, _, oldestEventDate, _) = projects.last
      val maxDistance                           = BigDecimal(oldestEventDate.distance(from = latestEventDate))

      def findPriority(eventDate: EventDate): Priority = maxDistance match {
        case maxDistance if maxDistance == 0 => MaxPriority
        case maxDistance =>
          Priority.safeApply(
            BigDecimal(oldestEventDate.distance(from = eventDate).toDouble) / maxDistance
          )
      }
      projects.map { case ProjectInfo(projectId, projectPath, eventDate, currentOccupancy) =>
        (ProjectIds(projectId, projectPath), findPriority(eventDate), currentOccupancy)
      }
  }

  private lazy val correctPrioritiesUsingOccupancyPerProject
      : List[(ProjectIds, Priority, Int Refined NonNegative)] => List[(ProjectIds, Priority)] = {
    case Nil                           => Nil
    case (project, priority, _) :: Nil => List(project -> priority)
    case prioritiesList =>
      val prioritiesCorrectedByOccupancy = prioritiesList.map(
        correctPriority(
          totalCapacity = subscribers.getTotalCapacity getOrElse Capacity((prioritiesList map toOccupancy).sum),
          totalPriority = (prioritiesList map toPriority).sum
        )
      )
      val maybePrioritiesAboveMinPriority = NonEmptyList.fromList {
        prioritiesCorrectedByOccupancy filter projectsAbove(MinPriority)
      }
      maybePrioritiesAboveMinPriority
        .getOrElse(NonEmptyList.of(prioritiesCorrectedByOccupancy.maxBy { case (_, priority) => priority }))
        .toList
        .map(alignItemType)
  }

  private def correctPriority(totalCapacity: Capacity,
                              totalPriority: BigDecimal
  ): ((ProjectIds, Priority, Int Refined NonNegative)) => (ProjectIds, BigDecimal) = {
    case (project, currentPriority, currentOccupancy) if currentOccupancy.value == 0 =>
      project -> currentPriority.value
    case (project, currentPriority, currentOccupancy) =>
      val maxOccupancy = currentPriority.value * totalCapacity.value / totalPriority
      val correctedPriority =
        if (currentOccupancy.value >= maxOccupancy) BigDecimal(0d)
        else currentPriority.value * (maxOccupancy / currentOccupancy.value)
      project -> correctedPriority
  }

  private def projectsAbove(min: Priority): ((ProjectIds, BigDecimal)) => Boolean = { case (_, priority) =>
    priority >= min.value
  }

  private lazy val alignItemType: ((ProjectIds, BigDecimal)) => (ProjectIds, Priority) = {
    case (projectIdAndPath, priority) => projectIdAndPath -> Priority.safeApply(priority)
  }

  private lazy val toPriority: ((ProjectIds, Priority, Int Refined NonNegative)) => BigDecimal = {
    case (_, priority, _) => priority.value
  }

  private lazy val toOccupancy: ((ProjectIds, Priority, Int Refined NonNegative)) => Int = { case (_, _, occupancy) =>
    occupancy.value
  }

  private implicit class EventDateOps(eventDate: EventDate) {
    def distance(from: EventDate): Long = Duration.between(from.value, eventDate.value).toHours
  }
}

private object ProjectPrioritisation {

  def apply[Interpretation[_]](subscribers: Subscribers[Interpretation])(implicit
      ME:                                   MonadError[Interpretation, Throwable]
  ): Interpretation[ProjectPrioritisation[Interpretation]] =
    ME.catchNonFatal(new ProjectPrioritisationImpl[Interpretation](subscribers))

  final case class ProjectInfo(id:               projects.Id,
                               path:             projects.Path,
                               latestEventDate:  EventDate,
                               currentOccupancy: Int Refined NonNegative
  )
  final class Priority private (val value: BigDecimal) extends AnyVal with BigDecimalTinyType
  object Priority extends TinyTypeFactory[Priority](new Priority(_)) {

    addConstraint(
      value => (value >= .1) && (value <= BigDecimal(1)),
      value => s"$value is not valid $typeName as it has to be >= 0.1 && <= 1"
    )

    val MaxPriority: Priority = Priority(1.0)
    val MinPriority: Priority = Priority(0.1)

    private def apply(value: Double): Priority = Priority(BigDecimal(value))

    def safeApply(value: BigDecimal): Priority = value match {
      case value if value <= .1  => MinPriority
      case value if value >= 1.0 => MaxPriority
      case value                 => Priority(value)
    }
    def safeApply(value: Double): Priority = safeApply(BigDecimal(value))
  }
}
