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

package io.renku.entities.searchgraphs.projects
package commands

import Generators._
import io.renku.entities.searchgraphs.Generators.personInfos
import io.renku.entities.searchgraphs.PersonInfo
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.positiveInts
import io.renku.generators.jsonld.JsonLDGenerators.entityIds
import io.renku.graph.model.GraphModelGenerators.imageUris
import io.renku.graph.model.Schemas.{rdf, renku, schema}
import io.renku.graph.model.entities.Person
import io.renku.graph.model.images.{Image, ImagePosition, ImageResourceId}
import io.renku.graph.model.projects
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EncodersSpec extends AnyWordSpec with should.Matchers {

  import Encoders._

  "personInfoEncoder" should {

    "turn a PersonInfo object into a Set of relevant Quads" in {

      val personInfo = personInfos.generateOne

      personInfo.asQuads shouldBe Set(
        ProjectsQuad(personInfo.resourceId, rdf / "type", Person.Ontology.typeClass.id),
        ProjectsQuad(personInfo.resourceId, Person.Ontology.nameProperty.id, personInfo.name.asObject)
      )
    }
  }

  "imageEncoder" should {

    "turn an Image object into a Set of relevant Quads" in {

      val image = {
        for {
          id       <- entityIds.toGeneratorOf(id => ImageResourceId(id.toString))
          uri      <- imageUris
          position <- positiveInts().toGeneratorOf(p => ImagePosition(p.value))
        } yield Image(id, uri, position)
      }.generateOne

      image.asQuads shouldBe Set(
        ProjectsQuad(image.resourceId, rdf / "type", schema / "ImageObject"),
        ProjectsQuad(image.resourceId, Image.Ontology.contentUrlProperty.id, image.uri.asObject),
        ProjectsQuad(image.resourceId, Image.Ontology.positionProperty.id, image.position.asObject)
      )
    }
  }

  "searchInfoEncoder" should {

    "turn a SearchInfo object into a Set of relevant Quads" in {

      val searchInfo = projectSearchInfoObjects.generateOne

      searchInfo.asQuads shouldBe Set(
        ProjectsQuad(searchInfo.id, rdf / "type", renku / "DiscoverableProject"),
        ProjectsQuad(searchInfo.id, ProjectSearchInfoOntology.nameProperty.id, searchInfo.name.asObject),
        ProjectsQuad(searchInfo.id, ProjectSearchInfoOntology.pathProperty.id, searchInfo.path.asObject),
        ProjectsQuad(searchInfo.id, ProjectSearchInfoOntology.visibilityProperty.id, searchInfo.visibility.asObject),
        ProjectsQuad(searchInfo.id, ProjectSearchInfoOntology.dateCreatedProperty.id, searchInfo.dateCreated.asObject)
      ) ++
        creatorToQuads(searchInfo) ++
        keywordsToQuads(searchInfo) ++
        maybeDescToQuad(searchInfo) ++
        imagesToQuads(searchInfo)
    }
  }

  private def creatorToQuads(searchInfo: ProjectSearchInfo): Set[Quad] =
    searchInfo.maybeCreator.toSet
      .flatMap((pi: PersonInfo) =>
        pi.asQuads + ProjectsQuad(searchInfo.id, ProjectSearchInfoOntology.creatorProperty, pi.resourceId.asEntityId)
      )

  private def keywordsToQuads(searchInfo: ProjectSearchInfo): Set[Quad] =
    searchInfo.keywords
      .map(k => ProjectsQuad(searchInfo.id, ProjectSearchInfoOntology.keywordsProperty.id, k.asObject))
      .toSet

  private def maybeDescToQuad(searchInfo: ProjectSearchInfo): Set[Quad] = searchInfo.maybeDescription.toSet.map {
    (d: projects.Description) =>
      ProjectsQuad(searchInfo.id, ProjectSearchInfoOntology.descriptionProperty.id, d.asObject)
  }

  private def imagesToQuads(searchInfo: ProjectSearchInfo): Set[Quad] =
    searchInfo.images
      .map(i =>
        i.asQuads + ProjectsQuad(searchInfo.id, ProjectSearchInfoOntology.imageProperty, i.resourceId.asEntityId)
      )
      .toSet
      .flatten
}
