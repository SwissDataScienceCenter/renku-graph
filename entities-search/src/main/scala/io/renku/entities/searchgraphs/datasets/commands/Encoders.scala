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

package io.renku.entities.searchgraphs.datasets
package commands

import DatasetSearchInfoOntology._
import Link.{ImportedDataset, OriginalDataset}
import cats.syntax.all._
import io.renku.entities.searchgraphs.maybeTripleObject
import io.renku.graph.model.Schemas.{rdf, renku}
import io.renku.graph.model.images.Image
import io.renku.graph.model.{datasets, persons}
import io.renku.jsonld.Property
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.{Quad, QuadsEncoder, TripleObject}
import io.renku.triplesstore.client.syntax._

private[datasets] object Encoders {

  implicit val linkEncoder: QuadsEncoder[Link] = QuadsEncoder.instance { link =>
    val typeQuads = link match {
      case _: OriginalDataset =>
        Set(DatasetsQuad(link.resourceId, rdf / "type", renku / "DatasetProjectLink"),
            DatasetsQuad(link.resourceId, rdf / "type", renku / "DatasetOriginalProjectLink")
        )
      case _: ImportedDataset =>
        Set(DatasetsQuad(link.resourceId, rdf / "type", renku / "DatasetProjectLink"))
    }
    typeQuads ++
      Set(
        DatasetsQuad(link.resourceId, LinkOntology.project, link.projectId.asEntityId),
        DatasetsQuad(link.resourceId, LinkOntology.dataset, link.datasetId.asEntityId)
      )
  }

  implicit val projectsVisibilitiesConcatEncoder: QuadsEncoder[(datasets.TopmostSameAs, List[Link])] =
    QuadsEncoder.instance { case (topSameAs, links) =>
      maybeTripleObject[Link](links, link => s"${link.projectSlug.value}:${link.visibility.value}")
        .map(DatasetsQuad(topSameAs, projectsVisibilitiesConcatProperty.id, _))
        .toSet
    }

  implicit val searchInfoEncoder: QuadsEncoder[DatasetSearchInfo] = QuadsEncoder.instance { info =>
    def searchInfoQuad(predicate: Property, obj: TripleObject): Quad =
      DatasetsQuad(info.topmostSameAs, predicate, obj)

    def maybeConcatQuad[A](property: Property, values: List[A], toValue: A => String): Option[Quad] =
      maybeTripleObject(values, toValue).map(searchInfoQuad(property, _))

    val createdOrPublishedQuad = info.createdOrPublished match {
      case d: datasets.DateCreated =>
        searchInfoQuad(dateCreatedProperty.id, d.asObject)
      case d: datasets.DatePublished =>
        searchInfoQuad(datePublishedProperty.id, d.asObject)
    }

    val maybeDateModifiedQuad = info.maybeDateModified.map { d =>
      searchInfoQuad(dateModifiedProperty.id, d.asObject)
    }

    val maybeDescriptionQuad = info.maybeDescription.map { d =>
      searchInfoQuad(descriptionProperty.id, d.asObject)
    }

    val creatorsQuads = info.creators.toList.toSet.map { (creator: Creator) =>
      searchInfoQuad(creatorProperty, creator.resourceId.asEntityId)
    }

    val maybeCreatorsNamesConcatQuad =
      maybeConcatQuad[persons.Name](creatorsNamesConcatProperty.id, info.creators.toList.map(_.name).distinct, _.value)

    val maybeKeywordsConcatQuad =
      maybeConcatQuad[datasets.Keyword](keywordsConcatProperty.id, info.keywords.distinct, _.value)

    val maybeImagesConcatQuad =
      maybeConcatQuad[Image](imagesConcatProperty.id,
                             info.images,
                             image => s"${image.position.value}:${image.uri.value}"
      )

    val linksQuads = info.links.toList.toSet.flatMap { (l: Link) =>
      l.asQuads +
        searchInfoQuad(linkProperty, l.resourceId.asEntityId)
    }

    val projectsVisibilitiesConcatQuads =
      (info.topmostSameAs -> info.links.toList).asQuads

    Set(
      searchInfoQuad(rdf / "type", typeDef.clazz.id).some,
      searchInfoQuad(nameProperty.id, info.name.asObject).some,
      searchInfoQuad(slugProperty.id, info.slug.asObject).some,
      searchInfoQuad(visibilityProperty.id, info.visibility.asObject).some,
      createdOrPublishedQuad.some,
      maybeDateModifiedQuad,
      maybeDescriptionQuad,
      maybeCreatorsNamesConcatQuad,
      maybeKeywordsConcatQuad,
      maybeImagesConcatQuad
    ).flatten ++ projectsVisibilitiesConcatQuads ++ creatorsQuads ++ linksQuads
  }
}
