/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.events.producers

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.eventlog.events.producers.DefaultSubscribers.DefaultSubscribers
import io.renku.eventlog.events.producers.ProjectPrioritisation._
import io.renku.events.consumers.Project
import io.renku.graph.model.events.EventDate
import io.renku.tinytypes.{BigDecimalTinyType, TinyTypeFactory}

import java.time.Duration

private trait ProjectPrioritisation[F[_]] {
  def prioritise(projects: List[ProjectInfo], totalOccupancy: Long): List[(ProjectIds, Priority)]
}

private class ProjectPrioritisationImpl[F[_]: DefaultSubscribers] extends ProjectPrioritisation[F] {
  import ProjectPrioritisation.Priority._

  private val FreeSpotsRatio = .15

  override def prioritise(projects: List[ProjectInfo], totalOccupancy: Long): List[(ProjectIds, Priority)] =
    (rejectProjectsAboveOccupancyThreshold >>>
      findPrioritiesBasedOnLatestEventDate >>>
      correctPrioritiesUsingOccupancyPerProject)(projects, totalOccupancy)

  private lazy val rejectProjectsAboveOccupancyThreshold: ((List[ProjectInfo], Long)) => List[ProjectInfo] = {
    case (projects, totalOccupancy) =>
      DefaultSubscribers[F].getTotalCapacity
        .map { totalCapacity =>
          val freeSpots =
            if ((totalCapacity * FreeSpotsRatio).ceil.intValue() < 2) 2
            else (totalCapacity * FreeSpotsRatio).ceil.intValue()

          projects match {
            case Nil => Nil
            case projects =>
              projects.foldLeft(List.empty[ProjectInfo]) { (acc, project) =>
                if (project.currentOccupancy.value == 0) acc :+ project
                else if (totalOccupancy < totalCapacity.value - freeSpots) acc :+ project
                else acc
              }
          }
        }
        .getOrElse(projects)
  }

  private lazy val findPrioritiesBasedOnLatestEventDate
      : List[ProjectInfo] => List[(ProjectIds, Priority, Int Refined NonNegative)] = {
    case Nil => Nil
    case ProjectInfo(project, _, currentOccupancy) :: Nil =>
      List((ProjectIds(project), MaxPriority, currentOccupancy))
    case projects =>
      val ProjectInfo(_, latestEventDate, _) = projects.head
      val ProjectInfo(_, oldestEventDate, _) = projects.last
      val maxDistance                        = BigDecimal(oldestEventDate.distance(from = latestEventDate))

      def findPriority(eventDate: EventDate): Priority = maxDistance match {
        case maxDistance if maxDistance == 0 => MaxPriority
        case maxDistance =>
          Priority.safeApply(
            BigDecimal(oldestEventDate.distance(from = eventDate).toDouble) / maxDistance
          )
      }
      projects.map { case ProjectInfo(project, eventDate, currentOccupancy) =>
        (ProjectIds(project), findPriority(eventDate), currentOccupancy)
      }
  }

  private lazy val correctPrioritiesUsingOccupancyPerProject
      : List[(ProjectIds, Priority, Int Refined NonNegative)] => List[(ProjectIds, Priority)] = {
    case Nil                           => Nil
    case (project, priority, _) :: Nil => List(project -> priority)
    case prioritiesList =>
      val prioritiesCorrectedByOccupancy = prioritiesList.map(
        correctPriority(
          totalCapacity =
            DefaultSubscribers[F].getTotalCapacity getOrElse TotalCapacity((prioritiesList map toOccupancy).sum),
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

  private def correctPriority(totalCapacity: TotalCapacity,
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
    case (projectIdAndSlug, priority) => projectIdAndSlug -> Priority.safeApply(priority)
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

  def apply[F[_]: MonadThrow: DefaultSubscribers]: F[ProjectPrioritisation[F]] =
    MonadThrow[F].catchNonFatal(new ProjectPrioritisationImpl[F])

  final case class ProjectInfo(project: Project, latestEventDate: EventDate, currentOccupancy: Int Refined NonNegative)
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
  }
}
