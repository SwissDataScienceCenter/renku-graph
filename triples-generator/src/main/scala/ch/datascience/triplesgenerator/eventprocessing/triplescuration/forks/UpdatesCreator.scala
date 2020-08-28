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

import cats.implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.projects.{DateCreated, Path, ResourceId}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.Email
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.rdfstore.SparqlValueEncoder.sparqlEncode
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.UpdateFunction
import eu.timepit.refined.auto._

private class UpdatesCreator(renkuBaseUrl: RenkuBaseUrl) {

  def recreateWasDerivedFrom(resourceId: ResourceId, forkPath: Path) = List {
    val rdfResource  = resourceId.showAs[RdfResource]
    val forkResource = ResourceId(renkuBaseUrl, forkPath).showAs[RdfResource]
    UpdateFunction(
      s"Updating Project $rdfResource prov:wasDerivedFrom",
      SparqlQuery(
        name = "upload - project derived update",
        Set("PREFIX prov: <http://www.w3.org/ns/prov#>"),
        s"""|DELETE { $rdfResource prov:wasDerivedFrom ?parentId }
            |INSERT { $rdfResource prov:wasDerivedFrom $forkResource }
            |WHERE  { $rdfResource prov:wasDerivedFrom ?parentId }
            |""".stripMargin
      )
    )
  }

  def deleteWasDerivedFrom(resourceId: ResourceId) = List {
    val rdfResource = resourceId.showAs[RdfResource]
    UpdateFunction(
      s"Deleting Project $rdfResource prov:wasDerivedFrom",
      SparqlQuery(
        name = "upload - project derived delete",
        Set("PREFIX prov: <http://www.w3.org/ns/prov#>"),
        s"""|DELETE { $rdfResource prov:wasDerivedFrom ?parentId }
            |WHERE  { $rdfResource prov:wasDerivedFrom ?parentId }
            |""".stripMargin
      )
    )
  }

  def insertWasDerivedFrom(resourceId: ResourceId, forkPath: Path) = List {
    val rdfResource  = resourceId.showAs[RdfResource]
    val forkResource = ResourceId(renkuBaseUrl, forkPath).showAs[RdfResource]
    UpdateFunction(
      s"Inserting Project $rdfResource prov:wasDerivedFrom",
      SparqlQuery(
        name = "upload - project derived insert",
        Set("PREFIX prov: <http://www.w3.org/ns/prov#>"),
        s"""INSERT DATA { $rdfResource prov:wasDerivedFrom $forkResource }"""
      )
    )
  }

  def swapCreator(resourceId: ResourceId, newResourceId: users.ResourceId): List[UpdateFunction] = List {
    val rdfResource     = resourceId.showAs[RdfResource]
    val creatorResource = newResourceId.showAs[RdfResource]
    UpdateFunction(
      s"Update Project $rdfResource schema:creator",
      SparqlQuery(
        name = "upload - project creator update",
        Set("PREFIX schema: <http://schema.org/>"),
        s"""|DELETE { $rdfResource schema:creator ?creatorId }
            |INSERT { $rdfResource schema:creator $creatorResource }
            |WHERE  { $rdfResource schema:creator ?creatorId }
            |""".stripMargin
      )
    )
  }

  def addNewCreator(resourceId:        ResourceId,
                    maybeCreatorEmail: Option[Email],
                    maybeCreatorName:  Option[users.Name]): List[UpdateFunction] = {
    val creatorResource = findCreatorId(maybeCreatorEmail)
    insertOrUpdateCreator(maybeCreatorEmail, maybeCreatorName) ++ swapCreator(resourceId, creatorResource)
  }

  private def insertOrUpdateCreator(maybeCreatorEmail: Option[Email], maybeCreatorName: Option[users.Name]) = {
    val creatorResource = findCreatorId(maybeCreatorEmail)
    List(
      maybeCreatorEmail -> maybeCreatorName match {
        case (Some(email), Some(name)) =>
          UpdateFunction(
            s"Updating Creator $creatorResource schema:email",
            SparqlQuery(
              name = "upload - creator update",
              Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "PREFIX schema: <http://schema.org/>"),
              s"""|DELETE { $creatorResource schema:name ?name }
                  |INSERT { $creatorResource rdf:type <http://schema.org/Person>;
                  |         schema:email '${sparqlEncode(email.value)};
                  |         schema:name '${sparqlEncode(name.value)}'.
                  |}
                  |WHERE  { $creatorResource schema:creator ?name }
                  |""".stripMargin
            )
          ).some
        case (Some(email), None) =>
          UpdateFunction(
            s"Inserting Creator $creatorResource schema:email",
            SparqlQuery(
              name = "upload - creator insert email",
              Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "PREFIX schema: <http://schema.org/>"),
              s"""|INSERT DATA {
                  |  $creatorResource rdf:type <http://schema.org/Person>;
                  |                   schema:email '${sparqlEncode(email.value)}'
                  |}""".stripMargin
            )
          ).some
        case (None, Some(name)) =>
          UpdateFunction(
            s"Inserting Creator $creatorResource schema:email",
            SparqlQuery(
              name = "upload - creator insert name",
              Set("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>", "PREFIX schema: <http://schema.org/>"),
              s"""|INSERT DATA {
                  |  $creatorResource rdf:type <http://schema.org/Person>;
                  |                   schema:name '${sparqlEncode(name.value)}'
                  |}""".stripMargin
            )
          ).some
        case _ => None
      }
    ).flatten
  }

  private lazy val findCreatorId: Option[Email] => users.ResourceId = {
    case None => users.ResourceId(s"_:${java.util.UUID.randomUUID()}")
    case Some(email) =>
      val username     = email.extractName.value
      val encodedEmail = email.value.replace(username, sparqlEncode(username))
      users.ResourceId(s"mailto:$encodedEmail")
  }

  def recreateDateCreated(resourceId: ResourceId, dateCreated: DateCreated) = {
    val rdfResource = resourceId.showAs[RdfResource]
    List(
      UpdateFunction(
        s"Deleting Project $rdfResource schema:dateCreated",
        SparqlQuery(
          name = "upload - project dateCreated delete",
          Set("PREFIX schema: <http://schema.org/>"),
          s"""|DELETE { $rdfResource schema:dateCreated ?date }
              |WHERE  { $rdfResource schema:dateCreated ?date }
              |""".stripMargin
        )
      ),
      UpdateFunction(
        s"Inserting Project $rdfResource schema:dateCreated",
        SparqlQuery(
          name = "upload - project dateCreated insert",
          Set("PREFIX schema: <http://schema.org/>", "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>"),
          s"""INSERT DATA { $rdfResource schema:dateCreated '$dateCreated'^^xsd:dateTime }"""
        )
      )
    )
  }
}
