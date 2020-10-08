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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.syntax.all._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{DateCreated, Path, ResourceId}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlValueEncoder.sparqlEncode
import eu.timepit.refined.auto._

private class UpdatesQueryCreator(renkuBaseUrl: RenkuBaseUrl) {

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

  def swapCreatorWithoutEmail(projectId: ResourceId, creatorName: users.Name): List[SparqlQuery] = {
    val rdfResource = projectId.showAs[RdfResource]
    List(
      SparqlQuery(
        name = "upload - project creator delete",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""|DELETE { $rdfResource schema:creator ?creatorId }
            |WHERE  { $rdfResource schema:creator ?creatorId }
            |""".stripMargin
      ),
      SparqlQuery(
        name = "upload - project creator insert",
        Set(
          "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
          "PREFIX schema: <http://schema.org/>"
        ),
        s"""|INSERT DATA {
            |  _:b0 rdf:type <http://schema.org/Person>;
            |       schema:name '${sparqlEncode(creatorName.toEscapedName.value)}'.
            |  $rdfResource schema:creator _:b0.
            |}
            |""".stripMargin
      )
    )
  }

  def addNewCreator(projectPath:       Path,
                    maybeCreatorEmail: Option[Email],
                    maybeCreatorName:  Option[users.Name]
  ): List[SparqlQuery] = {
    val projectId = ResourceId(renkuBaseUrl, projectPath)
    maybeCreatorEmail match {
      case Some(creatorEmail) =>
        val creatorResource = findCreatorId(creatorEmail)
        insertOrUpdateCreator(creatorResource, maybeCreatorEmail, maybeCreatorName) ++
          swapCreator(projectPath, creatorResource)
      case None =>
        maybeCreatorName match {
          case None              => List.empty
          case Some(creatorName) => swapCreatorWithoutEmail(projectId, creatorName)
        }
    }
  }

  private def insertOrUpdateCreator(
      personId:          users.ResourceId,
      maybeCreatorEmail: Option[Email],
      maybeCreatorName:  Option[users.Name]
  ): List[SparqlQuery] = {
    val creatorResource = personId.showAs[RdfResource]
    List(
      maybeCreatorEmail -> maybeCreatorName match {
        case (Some(email), Some(name)) =>
          SparqlQuery(
            name = "upload - creator update",
            Set(
              "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
              "PREFIX schema: <http://schema.org/>"
            ),
            s"""|DELETE { $creatorResource schema:name ?name }
                |INSERT { $creatorResource rdf:type <http://schema.org/Person>;
                |         schema:email '${sparqlEncode(email.value)}';
                |         schema:name '${sparqlEncode(name.toEscapedName.value)}' .
                |}
                |WHERE  { 
                |  OPTIONAL { $creatorResource schema:name ?maybeName} 
                |  BIND (IF(BOUND(?maybeName), ?maybeName, "nonexisting") AS ?name)
                |}
                |""".stripMargin
          ).some
        case (Some(email), None) =>
          SparqlQuery(
            name = "upload - creator insert email",
            Set(
              "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
              "PREFIX schema: <http://schema.org/>"
            ),
            s"""|INSERT DATA {
                |  $creatorResource rdf:type <http://schema.org/Person>;
                |                   schema:email '${sparqlEncode(email.value)}' .
                |}""".stripMargin
          ).some
        case (None, Some(name)) =>
          SparqlQuery(
            name = "upload - creator insert name",
            Set(
              "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
              "PREFIX schema: <http://schema.org/>"
            ),
            s"""|INSERT DATA {
                |  $creatorResource rdf:type <http://schema.org/Person>;
                |                   schema:name '${sparqlEncode(name.toEscapedName.value)}' .
                |}""".stripMargin
          ).some
        case _ => None
      }
    ).flatten
  }

  private def findCreatorId(email: Email): users.ResourceId = users.ResourceId(s"mailto:$email")

  def recreateDateCreated(projectPath: Path, dateCreated: DateCreated): List[SparqlQuery] = {
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
}
