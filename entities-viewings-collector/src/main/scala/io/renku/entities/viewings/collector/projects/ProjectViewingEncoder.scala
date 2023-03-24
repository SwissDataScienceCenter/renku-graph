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

package io.renku.entities.viewings.collector.projects

import io.renku.graph.model.{persons, projects, GraphClass}
import io.renku.jsonld._
import io.renku.jsonld.syntax._

private object ProjectViewingEncoder {

  final case class ProjectViewing(projectId: projects.ResourceId, dateViewed: projects.DateViewed)

  final case class Project(id: projects.ResourceId, path: projects.Path)

  final case class PersonViewedProject(userId: persons.ResourceId, project: Project, dateViewed: projects.DateViewed)

  def encode(viewing: ProjectViewing): NamedGraph =
    NamedGraph.fromJsonLDsUnsafe(
      GraphClass.ProjectViewedTimes.id,
      viewing.asJsonLD
    )

  def encode(viewing: PersonViewedProject): NamedGraph =
    NamedGraph.fromJsonLDsUnsafe(
      GraphClass.PersonViewings.id,
      viewing.asJsonLD
    )

  private implicit lazy val projectViewingEncoder: JsonLDEncoder[ProjectViewing] =
    JsonLDEncoder.instance { case ProjectViewing(entityId, date) =>
      JsonLD.entity(
        entityId.asEntityId,
        EntityTypes of ProjectViewedTimeOntology.classType,
        ProjectViewedTimeOntology.dataViewedProperty.id -> date.asJsonLD
      )
    }

  private implicit lazy val personViewedProjectEncoder: JsonLDEncoder[PersonViewedProject] =
    JsonLDEncoder.instance { case ev @ PersonViewedProject(userId, _, _) =>
      JsonLD.entity(
        userId.asEntityId,
        EntityTypes of PersonViewingOntology.classType,
        PersonViewingOntology.viewedProjectProperty -> ev.asJsonLD(viewedProjectEncoder)
      )
    }

  private lazy val viewedProjectEncoder: JsonLDEncoder[PersonViewedProject] =
    JsonLDEncoder.instance { case PersonViewedProject(userId, Project(id, path), date) =>
      JsonLD.entity(
        EntityId.of(s"$userId/$path"),
        EntityTypes of PersonViewedProjectOntology.classType,
        PersonViewedProjectOntology.projectProperty       -> id.asJsonLD,
        PersonViewedProjectOntology.dateViewedProperty.id -> date.asJsonLD
      )
    }
}
