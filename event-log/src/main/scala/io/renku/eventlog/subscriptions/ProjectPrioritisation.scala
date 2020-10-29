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

import ch.datascience.graph.model.projects
import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.NonNegative
import io.renku.eventlog.EventDate

private object ProjectPrioritisation {

  private val MaxPriority = BigDecimal(1.0)
  private val MinPriority = BigDecimal(0.1)

  final case class ProjectIdAndPath(id: projects.Id, path: projects.Path)
  final case class ProjectInfo(id:               projects.Id,
                               path:             projects.Path,
                               latestEventDate:  EventDate,
                               currentOccupancy: Int Refined NonNegative
  )

  def prioritise(projects: List[ProjectInfo]): List[(ProjectIdAndPath, BigDecimal)] =
    correctWeightsUsingOccupancyPerProject(findWeightsBasedOnMostRecentActivity(projects))

  private lazy val findWeightsBasedOnMostRecentActivity
      : List[ProjectInfo] => List[(ProjectIdAndPath, BigDecimal, Int Refined NonNegative)] = {
    case Nil => Nil
    case ProjectInfo(projectId, projectPath, _, currentOccupancy) :: Nil =>
      List((ProjectIdAndPath(projectId, projectPath), MaxPriority, currentOccupancy))
    case projects =>
      val ProjectInfo(_, _, latestEventDate, _) = projects.head
      val ProjectInfo(_, _, oldestEventDate, _) = projects.last
      val maxDistance                           = BigDecimal(latestEventDate.distance(from = oldestEventDate))

      def findWeight(eventDate: EventDate): BigDecimal = maxDistance match {
        case maxDistance if maxDistance == 0 => MaxPriority
        case maxDistance =>
          val normalisedDistance = BigDecimal(eventDate.distance(from = oldestEventDate).toDouble) / maxDistance
          if (normalisedDistance == 0) MinPriority
          else normalisedDistance
      }
      projects.map { case ProjectInfo(projectId, projectPath, eventDate, currentOccupancy) =>
        (ProjectIdAndPath(projectId, projectPath), findWeight(eventDate), currentOccupancy)
      }
  }

  private lazy val correctWeightsUsingOccupancyPerProject
      : List[(ProjectIdAndPath, BigDecimal, Int Refined NonNegative)] => List[(ProjectIdAndPath, BigDecimal)] = {
    case Nil => Nil
    case weightedList @ _ :: Nil =>
      weightedList.map { case (projectIdAndPath, weight, _) => (projectIdAndPath, weight) }
    case weightedList =>
      val totalWeight        = weightedList.map(_._2).sum
      val processingCapacity = weightedList.map(_._3.value).sum
      weightedList.map(correctWeight(processingCapacity, totalWeight))
  }

  private def correctWeight(processingCapacity: Int,
                            totalWeight:        BigDecimal
  ): ((ProjectIdAndPath, BigDecimal, Int Refined NonNegative)) => (ProjectIdAndPath, BigDecimal) = {
    case (project, currentWeight, currentOccupancy) if currentOccupancy.value == 0 => project -> currentWeight
    case (project, currentWeight, currentOccupancy) =>
      val maxOccupancy = currentWeight * processingCapacity / totalWeight
      val correction   = maxOccupancy / currentOccupancy.value
      val correctedWeight = currentWeight * correction match {
        case weight if weight < .1  => MinPriority
        case weight if weight > 1.0 => MaxPriority
        case weight                 => weight
      }
      project -> correctedWeight
  }

  private implicit class EventDateOps(eventDate: EventDate) {
    def distance(from: EventDate): Long = Duration.between(from.value, eventDate.value).toHours
  }

}
