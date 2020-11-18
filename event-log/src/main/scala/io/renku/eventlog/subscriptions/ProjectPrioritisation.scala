/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package io.renku.eventlog.subscriptions

import java.time.Duration

import cats.data.NonEmptyList
import ch.datascience.graph.model.projects
import ch.datascience.tinytypes.{BigDecimalTinyType, TinyTypeFactory}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.eventlog.EventDate
import io.renku.eventlog.subscriptions.ProjectPrioritisation.{Priority, ProjectIdAndPath, ProjectInfo}

private class ProjectPrioritisation {
  import Priority._

  def prioritise(projects: List[ProjectInfo]): List[(ProjectIdAndPath, Priority)] =
    correctPrioritiesUsingOccupancyPerProject(findPrioritiesBasedOnMostRecentActivity(projects))

  private lazy val findPrioritiesBasedOnMostRecentActivity
      : List[ProjectInfo] => List[(ProjectIdAndPath, Priority, Int Refined NonNegative)] = {
    case Nil => Nil
    case ProjectInfo(projectId, projectPath, _, currentOccupancy) :: Nil =>
      List((ProjectIdAndPath(projectId, projectPath), MaxPriority, currentOccupancy))
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
        (ProjectIdAndPath(projectId, projectPath), findPriority(eventDate), currentOccupancy)
      }
  }

  private lazy val correctPrioritiesUsingOccupancyPerProject
      : List[(ProjectIdAndPath, Priority, Int Refined NonNegative)] => List[(ProjectIdAndPath, Priority)] = {
    case Nil                           => Nil
    case (project, priority, _) :: Nil => List(project -> priority)
    case prioritiesList =>
      val prioritiesCorrectedByOccupancy = prioritiesList.map(
        correctPriority(
          processingCapacity = (prioritiesList map toOccupancy).sum,
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

  private def correctPriority(processingCapacity: Int,
                              totalPriority:      BigDecimal
  ): ((ProjectIdAndPath, Priority, Int Refined NonNegative)) => (ProjectIdAndPath, BigDecimal) = {
    case (project, currentPriority, currentOccupancy) if currentOccupancy.value == 0 =>
      project -> currentPriority.value
    case (project, currentPriority, currentOccupancy) =>
      val maxOccupancy = currentPriority.value * processingCapacity / totalPriority
      val correctedPriority =
        if (currentOccupancy.value >= maxOccupancy) BigDecimal(0d)
        else currentPriority.value * (maxOccupancy / currentOccupancy.value)
      project -> correctedPriority
  }

  private def projectsAbove(min: Priority): ((ProjectIdAndPath, BigDecimal)) => Boolean = { case (_, priority) =>
    priority >= min.value
  }

  private lazy val alignItemType: ((ProjectIdAndPath, BigDecimal)) => (ProjectIdAndPath, Priority) = {
    case (projectIdAndPath, priority) => projectIdAndPath -> Priority.safeApply(priority)
  }

  private lazy val toPriority: ((ProjectIdAndPath, Priority, Int Refined NonNegative)) => BigDecimal = {
    case (_, priority, _) => priority.value
  }

  private lazy val toOccupancy: ((ProjectIdAndPath, Priority, Int Refined NonNegative)) => Int = {
    case (_, _, occupancy) => occupancy.value
  }

  private implicit class EventDateOps(eventDate: EventDate) {
    def distance(from: EventDate): Long = Duration.between(from.value, eventDate.value).toHours
  }
}

private object ProjectPrioritisation {

  final case class ProjectIdAndPath(id: projects.Id, path: projects.Path)
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
