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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated
package triplescuration.projects

import ch.datascience.graph.model.Schemas._
import ch.datascience.graph.model.entities.{Project, ProjectWithParent, ProjectWithoutParent}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects.KGProjectFinder.KGProjectInfo
import eu.timepit.refined.auto._

private trait UpdatesCreator {
  def prepareUpdates(project: Project, kgProjectInfo: KGProjectInfo): List[SparqlQuery]
}

private object UpdatesCreator extends UpdatesCreator {

  override def prepareUpdates(
      project:       Project,
      kgProjectInfo: KGProjectInfo
  ): List[SparqlQuery] = List(nameDeletion(project, kgProjectInfo),
                              maybeParentDeletion(project, kgProjectInfo),
                              visibilityDeletion(project, kgProjectInfo)
  ).flatten

  private def nameDeletion(project: Project, kgProjectInfo: KGProjectInfo) =
    Option.when(project.name != kgProjectInfo._1) {
      val resource = project.resourceId.showAs[RdfResource]
      SparqlQuery.of(
        name = "transformation - project name delete",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { $resource schema:name ?name }
            |WHERE  { $resource schema:name ?name }
            |""".stripMargin
      )
    }

  private def maybeParentDeletion(project: Project, kgProjectInfo: KGProjectInfo): Option[SparqlQuery] = project match {
    case p: ProjectWithParent =>
      Option.when(kgProjectInfo._2.isEmpty || kgProjectInfo._2.exists(_ != p.parentResourceId)) {
        val resource = project.resourceId.showAs[RdfResource]
        SparqlQuery.of(
          name = "transformation - project maybeParent delete",
          Prefixes.of(prov -> "prov"),
          s"""|DELETE { $resource prov:wasDerivedFrom ?maybeParent }
              |WHERE  { $resource prov:wasDerivedFrom ?maybeParent }
              |""".stripMargin
        )
      }
    case _: ProjectWithoutParent => None
  }

  private def visibilityDeletion(project: Project, kgProjectInfo: KGProjectInfo) = Option.when(
    project.visibility != kgProjectInfo._3
  ) {
    val resource = project.resourceId.showAs[RdfResource]
    SparqlQuery.of(
      name = "transformation - project visibility delete",
      Prefixes.of(renku -> "renku"),
      s"""|DELETE { $resource renku:projectVisibility ?visibility }
          |WHERE  { $resource renku:projectVisibility ?visibility }
          |""".stripMargin
    )
  }
}
