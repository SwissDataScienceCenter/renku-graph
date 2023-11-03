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

package io.renku.entities.searchgraphs
package projects
package commands

import Generators._
import ProjectSearchInfoOntology._
import cats.syntax.all._
import io.renku.graph.model.Schemas.{rdf, renku}
import io.renku.graph.model.{persons, projects}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.Quad
import io.renku.triplesstore.client.syntax._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class EncodersSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  import Encoders._

  "searchInfoEncoder" should {

    "turn a SearchInfo object into a Set of relevant Quads" in {

      forAll(projectSearchInfoObjects) { searchInfo =>
        searchInfo.asQuads shouldBe Set(
          ProjectsQuad(searchInfo.id, rdf / "type", renku / "DiscoverableProject"),
          ProjectsQuad(searchInfo.id, nameProperty.id, searchInfo.name.asObject),
          ProjectsQuad(searchInfo.id, slugProperty.id, searchInfo.slug.asObject),
          ProjectsQuad(searchInfo.id, pathProperty.id, searchInfo.slug.asObject),
          ProjectsQuad(searchInfo.id, visibilityProperty.id, searchInfo.visibility.asObject),
          ProjectsQuad(searchInfo.id, dateCreatedProperty.id, searchInfo.dateCreated.asObject),
          ProjectsQuad(searchInfo.id, dateModifiedProperty.id, searchInfo.dateModified.asObject)
        ) ++
          creatorToQuads(searchInfo) ++
          maybeDescToQuad(searchInfo) ++
          maybeKeywordsConcatToQuad(searchInfo).toSet ++
          maybeImagesConcatToQuad(searchInfo).toSet
      }
    }

  }

  private def creatorToQuads(searchInfo: ProjectSearchInfo): Set[Quad] =
    searchInfo.maybeCreator.toSet.map { (resourceId: persons.ResourceId) =>
      ProjectsQuad(searchInfo.id, creatorProperty, resourceId.asEntityId)
    }

  private def maybeDescToQuad(searchInfo: ProjectSearchInfo): Set[Quad] = searchInfo.maybeDescription.toSet.map {
    (d: projects.Description) =>
      ProjectsQuad(searchInfo.id, descriptionProperty.id, d.asObject)
  }

  private def maybeKeywordsConcatToQuad(searchInfo: ProjectSearchInfo): Option[Quad] =
    searchInfo.keywords match {
      case Nil => Option.empty[Quad]
      case k =>
        ProjectsQuad(searchInfo.id, keywordsConcatProperty.id, k.mkString(concatSeparator.toString).asTripleObject).some
    }

  private def maybeImagesConcatToQuad(searchInfo: ProjectSearchInfo): Option[Quad] =
    searchInfo.images match {
      case Nil => Option.empty[Quad]
      case images =>
        ProjectsQuad(searchInfo.id,
                     imagesConcatProperty.id,
                     images.map(i => s"${i.position}:${i.uri}").mkString(concatSeparator.toString).asTripleObject
        ).some
    }
}
