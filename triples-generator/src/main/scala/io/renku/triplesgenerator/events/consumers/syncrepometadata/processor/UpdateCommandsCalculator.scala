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

package io.renku.triplesgenerator.events.consumers.syncrepometadata
package processor

import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.eventlog.api.events.StatusChangeEvent
import io.renku.graph.model.Schemas.{rdf, schema}
import io.renku.graph.model.images.Image
import io.renku.graph.model.{GraphClass, projects}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.sparql.Fragment
import io.renku.triplesstore.client.syntax._

private trait UpdateCommandsCalculator {
  def calculateUpdateCommands(tsData:           DataExtract.TS,
                              glData:           DataExtract.GL,
                              maybePayloadData: Option[DataExtract.Payload]
  ): List[UpdateCommand]
}

private object UpdateCommandsCalculator {
  def apply(): UpdateCommandsCalculator = new UpdateCommandsCalculatorImpl(NewValuesCalculator)
}

private class UpdateCommandsCalculatorImpl(newValuesCalculator: NewValuesCalculator) extends UpdateCommandsCalculator {

  override def calculateUpdateCommands(tsData:           DataExtract.TS,
                                       glData:           DataExtract.GL,
                                       maybePayloadData: Option[DataExtract.Payload]
  ): List[UpdateCommand] = {
    val newValues = newValuesCalculator.findNewValues(tsData, glData, maybePayloadData)

    (
      newValues.maybeName.map(nameUpdates(tsData.id, _)) combine
        newValues.maybeVisibility.as(List(eventUpdate(tsData.path))) combine
        newValues.maybeDesc.map(descUpdates(tsData.id, _)) combine
        newValues.maybeKeywords.map(keywordsUpdates(tsData.id, _)) combine
        newValues.maybeImages.map(imagesUpdates(tsData.id, _))
    ).getOrElse(Nil)
  }

  private def nameUpdates(id: projects.ResourceId, newValue: projects.Name): List[UpdateCommand] = List(
    nameInProjectUpdate(id, newValue),
    nameInProjectsUpdate(id, newValue)
  ).map(UpdateCommand.Sparql)

  private def nameInProjectUpdate(id: projects.ResourceId, newValue: projects.Name) =
    SparqlQuery.ofUnsafe(
      show"$categoryName: update name in Project",
      Prefixes of schema -> "schema",
      sparql"""|DELETE { GRAPH ?id { ?id schema:name ?name } }
               |INSERT { GRAPH ?id { ?id schema:name ${newValue.asObject} } }
               |WHERE {
               |  BIND (${id.asEntityId} AS ?id)
               |  GRAPH ?id {
               |    ?id schema:name ?name
               |  }
               |}""".stripMargin
    )

  private def nameInProjectsUpdate(id: projects.ResourceId, newValue: projects.Name) =
    SparqlQuery.ofUnsafe(
      show"$categoryName: update name in Projects",
      Prefixes of schema -> "schema",
      sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} { ?id schema:name ?name } }
               |INSERT { GRAPH ${GraphClass.Projects.id} { ?id schema:name ${newValue.asObject} } }
               |WHERE {
               |  BIND (${id.asEntityId} AS ?id)
               |  GRAPH ${GraphClass.Projects.id} {
               |    ?id schema:name ?name
               |  }
               |}""".stripMargin
    )

  private def eventUpdate(projectPath: projects.Path): UpdateCommand =
    UpdateCommand.Event(StatusChangeEvent.RedoProjectTransformation(projectPath))

  private def descUpdates(id: projects.ResourceId, newValue: Option[projects.Description]): List[UpdateCommand] = List(
    descInProjectUpdate(id, newValue),
    descInProjectsUpdate(id, newValue)
  ).map(UpdateCommand.Sparql)

  private def descInProjectUpdate(id: projects.ResourceId, newValue: Option[projects.Description]) =
    newValue match {
      case Some(value) =>
        SparqlQuery.ofUnsafe(
          show"$categoryName: update desc in Project",
          Prefixes of schema -> "schema",
          sparql"""|DELETE { GRAPH ?id { ?id schema:description ?maybeDesc } }
                   |INSERT { GRAPH ?id { ?id schema:description ${value.asObject} } }
                   |WHERE {
                   |  BIND (${id.asEntityId} AS ?id)
                   |  GRAPH ?id {
                   |    OPTIONAL { ?id schema:description ?maybeDesc }
                   |  }
                   |}""".stripMargin
        )
      case None =>
        SparqlQuery.ofUnsafe(
          show"$categoryName: delete desc in Project",
          Prefixes of schema -> "schema",
          sparql"""|DELETE { GRAPH ?id { ?id schema:description ?maybeDesc } }
                   |WHERE {
                   |  BIND (${id.asEntityId} AS ?id)
                   |  GRAPH ?id {
                   |    OPTIONAL { ?id schema:description ?maybeDesc }
                   |  }
                   |}""".stripMargin
        )
    }

  private def descInProjectsUpdate(id: projects.ResourceId, newValue: Option[projects.Description]) =
    newValue match {
      case Some(value) =>
        SparqlQuery.ofUnsafe(
          show"$categoryName: update desc in Projects",
          Prefixes of schema -> "schema",
          sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} { ?id schema:description ?maybeDesc } }
                   |INSERT { GRAPH ${GraphClass.Projects.id} { ?id schema:description ${value.asObject} } }
                   |WHERE {
                   |  BIND (${id.asEntityId} AS ?id)
                   |  GRAPH ${GraphClass.Projects.id} {
                   |    OPTIONAL { ?id schema:description ?maybeDesc }
                   |  }
                   |}""".stripMargin
        )
      case None =>
        SparqlQuery.ofUnsafe(
          show"$categoryName: delete desc in Projects",
          Prefixes of schema -> "schema",
          sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} { ?id schema:description ?maybeDesc } }
                   |WHERE {
                   |  BIND (${id.asEntityId} AS ?id)
                   |  GRAPH ${GraphClass.Projects.id} {
                   |    OPTIONAL { ?id schema:description ?maybeDesc }
                   |  }
                   |}""".stripMargin
        )
    }

  private def keywordsUpdates(id: projects.ResourceId, newValue: Set[projects.Keyword]): List[UpdateCommand] = List(
    keywordsInProjectUpdate(id, newValue),
    keywordsInProjectsUpdate(id, newValue)
  ).map(UpdateCommand.Sparql)

  private def keywordsInProjectUpdate(id: projects.ResourceId, newValue: Set[projects.Keyword]) =
    SparqlQuery.ofUnsafe(
      show"$categoryName: update keywords in Project",
      Prefixes of schema -> "schema",
      sparql"""|DELETE { GRAPH ?id { ?id schema:keywords ?keyword } }
               |INSERT { GRAPH ?id {
               |  ${newValue.map(k => fr"""?id schema:keywords ${k.asObject}.""").toList.intercalate(fr"\n ")}
               |} }
               |WHERE {
               |  BIND (${id.asEntityId} AS ?id)
               |  GRAPH ?id {
               |    OPTIONAL { ?id schema:keywords ?keyword }
               |  }
               |}""".stripMargin
    )

  private def keywordsInProjectsUpdate(id: projects.ResourceId, newValue: Set[projects.Keyword]) =
    SparqlQuery.ofUnsafe(
      show"$categoryName: update keywords in Projects",
      Prefixes of schema -> "schema",
      sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} { ?id schema:keywords ?keyword } }
               |INSERT { GRAPH ${GraphClass.Projects.id} {
               |  ${newValue.map(k => fr"""?id schema:keywords ${k.asObject}.""").toList.intercalate(fr"\n")}
               |} }
               |WHERE {
               |  BIND (${id.asEntityId} AS ?id)
               |  GRAPH ${GraphClass.Projects.id} {
               |    OPTIONAL { ?id schema:keywords ?keyword }
               |  }
               |}""".stripMargin
    )

  private def imagesUpdates(id: projects.ResourceId, newValue: List[Image]): List[UpdateCommand] = List(
    imagesInProjectUpdate(id, newValue),
    imagesInProjectsUpdate(id, newValue)
  ).map(UpdateCommand.Sparql)

  private def imagesInProjectUpdate(id: projects.ResourceId, newValue: List[Image]) =
    SparqlQuery.ofUnsafe(
      show"$categoryName: update images in Project",
      Prefixes of (rdf -> "rdf", schema -> "schema"),
      sparql"""|DELETE { GRAPH ?id {
               |  ?id schema:image ?imageId.
               |  ?imageId ?p ?o
               |} }
               |INSERT { GRAPH ?id {
               |  ${newValue.flatMap(toTriple).intercalate(fr"\n ")}
               |} }
               |WHERE {
               |  BIND (${id.asEntityId} AS ?id)
               |  GRAPH ?id {
               |    OPTIONAL {
               |      ?id schema:image ?imageId.
               |      ?imageId ?p ?o
               |    }
               |  }
               |}""".stripMargin
    )

  private def imagesInProjectsUpdate(id: projects.ResourceId, newValue: List[Image]) =
    SparqlQuery.ofUnsafe(
      show"$categoryName: update keywords in Projects",
      Prefixes of (rdf -> "rdf", schema -> "schema"),
      sparql"""|DELETE { GRAPH ${GraphClass.Projects.id} {
               |  ?id schema:image ?imageId.
               |  ?imageId ?p ?o
               |} }
               |INSERT { GRAPH ${GraphClass.Projects.id} {
               |  ${newValue.flatMap(toTriple).intercalate(fr"\n ")}
               |} }
               |WHERE {
               |  BIND (${id.asEntityId} AS ?id)
               |  GRAPH ${GraphClass.Projects.id} {
               |    OPTIONAL {
               |      ?id schema:image ?imageId.
               |      ?imageId ?p ?o
               |    }
               |  }
               |}""".stripMargin
    )

  private lazy val toTriple: Image => List[Fragment] = { case Image(resourceId, uri, position) =>
    List(
      fr"""?id schema:image ${resourceId.asEntityId}.""",
      fr"""${resourceId.asEntityId} rdf:type ${Image.Ontology.typeClass.id}.""",
      fr"""${resourceId.asEntityId} ${Image.Ontology.contentUrlProperty.id} ${uri.asObject}.""",
      fr"""${resourceId.asEntityId} ${Image.Ontology.positionProperty.id} ${position.asObject}."""
    )
  }
}
