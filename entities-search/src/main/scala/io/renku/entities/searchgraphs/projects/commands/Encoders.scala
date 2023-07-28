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

import cats.syntax.all._
import io.renku.entities.searchgraphs.PersonInfo
import io.renku.graph.model.Schemas.rdf
import io.renku.graph.model.entities.Person
import io.renku.graph.model.images.Image
import io.renku.graph.model.projects
import io.renku.jsonld.Property
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.{Quad, QuadsEncoder, TripleObject}
import io.renku.triplesstore.client.syntax._

private object Encoders {

  implicit val personInfoEncoder: QuadsEncoder[PersonInfo] = QuadsEncoder.instance {
    case PersonInfo(resourceId, name) =>
      Set(
        ProjectsQuad(resourceId, rdf / "type", Person.Ontology.typeClass.id),
        ProjectsQuad(resourceId, Person.Ontology.nameProperty.id, name.asObject)
      )
  }

  implicit val imageEncoder: QuadsEncoder[Image] = QuadsEncoder.instance { case Image(resourceId, uri, position) =>
    Set(
      ProjectsQuad(resourceId, rdf / "type", Image.Ontology.typeClass.id),
      ProjectsQuad(resourceId, Image.Ontology.contentUrlProperty.id, uri.asObject),
      ProjectsQuad(resourceId, Image.Ontology.positionProperty.id, position.asObject)
    )
  }

  implicit val searchInfoEncoder: QuadsEncoder[ProjectSearchInfo] = QuadsEncoder.instance { info =>
    def searchInfoQuad(predicate: Property, obj: TripleObject): Quad =
      ProjectsQuad(info.id, predicate, obj)

    val maybeDescriptionQuad = info.maybeDescription.map { d =>
      searchInfoQuad(ProjectSearchInfoOntology.descriptionProperty.id, d.asObject)
    }

    val creatorQuads = info.maybeCreator.toSet.flatMap { (c: PersonInfo) =>
      c.asQuads + searchInfoQuad(ProjectSearchInfoOntology.creatorProperty, c.resourceId.asEntityId)
    }

    val keywordsQuads = info.keywords.toSet.map { (k: projects.Keyword) =>
      searchInfoQuad(ProjectSearchInfoOntology.keywordsProperty.id, k.asObject)
    }

    val imagesQuads = info.images.toSet.flatMap { (i: Image) =>
      i.asQuads + searchInfoQuad(ProjectSearchInfoOntology.imageProperty, i.resourceId.asEntityId)
    }

    Set(
      searchInfoQuad(rdf / "type", ProjectSearchInfoOntology.typeDef.clazz.id).some,
      searchInfoQuad(ProjectSearchInfoOntology.nameProperty.id, info.name.asObject).some,
      searchInfoQuad(ProjectSearchInfoOntology.pathProperty.id, info.slug.asObject).some,
      searchInfoQuad(ProjectSearchInfoOntology.visibilityProperty.id, info.visibility.asObject).some,
      searchInfoQuad(ProjectSearchInfoOntology.dateCreatedProperty.id, info.dateCreated.asObject).some,
      searchInfoQuad(ProjectSearchInfoOntology.dateModifiedProperty.id, info.dateModified.asObject).some,
      maybeDescriptionQuad
    ).flatten ++ creatorQuads ++ keywordsQuads ++ imagesQuads
  }
}
