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

package io.renku.eventlog.events.categories

import ch.datascience.graph.model.events.EventStatus.{GeneratingTriples, TransformingTriples}
import ch.datascience.graph.model.events.{CategoryName, CompoundEventId, EventStatus}
import ch.datascience.graph.model.projects

package object zombieevents {
  private[zombieevents] val categoryName: CategoryName = CategoryName("ZOMBIE_CHASING")

  private[zombieevents] sealed trait ZombieEvent {
    type Status <: EventStatus
    val eventId:     CompoundEventId
    val projectPath: projects.Path
    val status:      Status
  }

  private[zombieevents] final case class GeneratingTriplesZombieEvent(eventId:     CompoundEventId,
                                                                      projectPath: projects.Path
  ) extends ZombieEvent {
    override type Status = GeneratingTriples
    override val status: GeneratingTriples = GeneratingTriples
  }

  private[zombieevents] final case class TransformingTriplesZombieEvent(eventId:     CompoundEventId,
                                                                        projectPath: projects.Path
  ) extends ZombieEvent {
    override type Status = TransformingTriples
    override val status: TransformingTriples = TransformingTriples
  }

  private[zombieevents] sealed trait UpdateResult
  private[zombieevents] case object Updated    extends UpdateResult
  private[zombieevents] case object NotUpdated extends UpdateResult

}
