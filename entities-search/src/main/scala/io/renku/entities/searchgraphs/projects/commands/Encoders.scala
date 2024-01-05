/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import ProjectSearchInfoOntology._
import cats.syntax.all._
import io.renku.graph.model.Schemas.rdf
import io.renku.graph.model.images.Image
import io.renku.graph.model.{persons, projects}
import io.renku.jsonld.Property
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.{Quad, QuadsEncoder, TripleObject}
import io.renku.triplesstore.client.syntax._

object Encoders {

  def maybeKeywordsObject(keywords: List[projects.Keyword]): Option[TripleObject] =
    maybeTripleObject[projects.Keyword](keywords.distinct, _.value)

  def maybeKeywordsQuad(id: projects.ResourceId, keywords: List[projects.Keyword]): Option[Quad] =
    maybeKeywordsObject(keywords).map(ProjectsQuad(id, keywordsConcatProperty.id, _))

  def maybeImagesObject(images: List[Image]): Option[TripleObject] =
    maybeTripleObject[Image](images, image => s"${image.position.value}:${image.uri.value}")

  def maybeImagesQuad(id: projects.ResourceId, images: List[Image]): Option[Quad] =
    maybeImagesObject(images).map(ProjectsQuad(id, imagesConcatProperty.id, _))

  private[commands] implicit val searchInfoEncoder: QuadsEncoder[ProjectSearchInfo] = QuadsEncoder.instance { info =>
    def searchInfoQuad(predicate: Property, obj: TripleObject): Quad =
      ProjectsQuad(info.id, predicate, obj)

    val maybeDescriptionQuad = info.maybeDescription.map { d =>
      searchInfoQuad(descriptionProperty.id, d.asObject)
    }

    val creatorQuads = info.maybeCreator.toSet.map { (resourceId: persons.ResourceId) =>
      searchInfoQuad(creatorProperty, resourceId.asEntityId)
    }

    val maybeKeywordsConcatQuad = maybeKeywordsQuad(info.id, info.keywords)

    val maybeImagesConcatQuad = maybeImagesQuad(info.id, info.images)

    Set(
      searchInfoQuad(rdf / "type", typeDef.clazz.id).some,
      searchInfoQuad(nameProperty.id, info.name.asObject).some,
      searchInfoQuad(slugProperty.id, info.slug.asObject).some,
      searchInfoQuad(pathProperty.id, info.slug.asObject).some,
      searchInfoQuad(visibilityProperty.id, info.visibility.asObject).some,
      searchInfoQuad(dateCreatedProperty.id, info.dateCreated.asObject).some,
      searchInfoQuad(dateModifiedProperty.id, info.dateModified.asObject).some,
      maybeKeywordsConcatQuad,
      maybeImagesConcatQuad,
      maybeDescriptionQuad
    ).flatten ++ creatorQuads
  }
}
