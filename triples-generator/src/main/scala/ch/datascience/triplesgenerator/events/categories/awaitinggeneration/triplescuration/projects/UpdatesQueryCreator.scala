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

package ch.datascience.triplesgenerator.events.categories.awaitinggeneration.triplescuration.projects

import ch.datascience.graph.Schemas._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.projects.{DateCreated, Path, ResourceId, Visibility}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore.SparqlValueEncoder.sparqlEncode
import eu.timepit.refined.auto._

private class UpdatesQueryCreator(renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl) {

  def updateWasDerivedFrom(projectPath: Path, maybeForkPath: Option[Path]) = List {
    val rdfResource = ResourceId(renkuBaseUrl, projectPath).showAs[RdfResource]
    maybeForkPath match {
      case Some(forkPath) =>
        val q = SparqlQuery(
          name = "upload - project derived update",
          Set("PREFIX prov: <http://www.w3.org/ns/prov#>"),
          s"""|DELETE { $rdfResource prov:wasDerivedFrom ?parentId }
              |INSERT { $rdfResource prov:wasDerivedFrom ${ResourceId(renkuBaseUrl, forkPath).showAs[RdfResource]} }
              |WHERE  { 
              | OPTIONAL { $rdfResource prov:wasDerivedFrom ?maybeParentId } 
              |  BIND (IF(BOUND(?maybeParentId), ?maybeParentId, "nonexisting") AS ?parentId)
              |}
              |""".stripMargin
        )
        q
      case _ =>
        SparqlQuery(
          name = "upload - project derived delete",
          Set("PREFIX prov: <http://www.w3.org/ns/prov#>"),
          s"""|DELETE { $rdfResource prov:wasDerivedFrom ?parentId }
              |WHERE  { $rdfResource prov:wasDerivedFrom ?parentId }
              |""".stripMargin
        )
    }
  }

  def swapCreator(projectPath: Path, creatorId: users.ResourceId): List[SparqlQuery] = {
    val rdfResource     = ResourceId(renkuBaseUrl, projectPath).showAs[RdfResource]
    val creatorResource = creatorId.showAs[RdfResource]
    List(
      SparqlQuery(
        name = "upload - project creator update",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""|DELETE { $rdfResource schema:creator ?creatorId }
            |INSERT { $rdfResource schema:creator $creatorResource }
            |WHERE  { 
            |  OPTIONAL { $rdfResource schema:creator ?maybeCreatorId } 
            |  BIND (IF(BOUND(?maybeCreatorId), ?maybeCreatorId, "nonexisting") AS ?creatorId)
            |}
            |""".stripMargin
      )
    )
  }

  def unlinkCreator(projectPath: Path): List[SparqlQuery] = {
    val rdfResource = ResourceId(renkuBaseUrl, projectPath).showAs[RdfResource]
    List(
      SparqlQuery(
        name = "upload - project creator unlink",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""|DELETE { $rdfResource schema:creator ?creatorId }
            |WHERE  { 
            |  OPTIONAL { $rdfResource schema:creator ?maybeCreatorId } 
            |  BIND (IF(BOUND(?maybeCreatorId), ?maybeCreatorId, "nonexisting") AS ?creatorId)
            |}
            |""".stripMargin
      )
    )
  }

  def addNewCreator(projectPath: Path, creator: GitLabCreator): List[SparqlQuery] = {
    val creatorResourceId = users.ResourceId(
      (renkuBaseUrl / "persons" / s"gitlabid${creator.gitLabId}").toString
    )
    insertCreator(creatorResourceId, creator) ++
      swapCreator(projectPath, creatorResourceId)
  }

  private def insertCreator(personId: users.ResourceId, creator: GitLabCreator): List[SparqlQuery] = {
    val creatorResource = personId.showAs[RdfResource]
    val sameAsId        = (gitLabApiUrl / "users" / creator.gitLabId).showAs[RdfResource]
    List(
      SparqlQuery
        .of(
          name = "upload - creator update",
          Prefixes.of(rdf -> "rdf", schema -> "schema"),
          s"""|INSERT DATA { 
              |  $creatorResource rdf:type <http://schema.org/Person>;
              |                   schema:name '${sparqlEncode(creator.name.value)}';
              |                   schema:sameAs $sameAsId.
              |  $sameAsId rdf:type schema:URL;
              |            schema:identifier ${creator.gitLabId};
              |            schema:additionalType 'GitLab'.
              |}
              |""".stripMargin
        )
    )
  }

  def updateDateCreated(projectPath: Path, dateCreated: DateCreated): List[SparqlQuery] = {
    val rdfResource = ResourceId(renkuBaseUrl, projectPath).showAs[RdfResource]
    List(
      SparqlQuery(
        name = "upload - project dateCreated updated",
        Set("PREFIX schema: <http://schema.org/>", "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"),
        s"""|DELETE { $rdfResource schema:dateCreated ?date }
            |INSERT { $rdfResource schema:dateCreated '$dateCreated'^^xsd:dateTime }
            |WHERE  {
            |  OPTIONAL { $rdfResource schema:dateCreated ?maybeDate } 
            |  BIND (IF(BOUND(?maybeDate), ?maybeDate, "nonexisting") AS ?date)
            |}
            |""".stripMargin
      )
    )
  }

  def upsertVisibility(projectPath: Path, visibility: Visibility): List[SparqlQuery] = {
    val rdfResource = ResourceId(renkuBaseUrl, projectPath).showAs[RdfResource]
    List(
      SparqlQuery.of(
        name = "upsert - project visibility",
        Prefixes.of(schema -> "schema"),
        s"""|DELETE { $rdfResource schema:additionalType ?visibility }
            |INSERT { $rdfResource schema:additionalType '$visibility' }
            |WHERE  {
            |  OPTIONAL { $rdfResource schema:additionalType ?maybeVisibility } 
            |  BIND (IF(BOUND(?maybeVisibility), ?maybeVisibility, "nonexisting") AS ?visibility)
            |}
            |""".stripMargin
      )
    )
  }
}
