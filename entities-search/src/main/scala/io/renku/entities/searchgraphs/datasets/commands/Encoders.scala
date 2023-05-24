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

package io.renku.entities.searchgraphs.datasets.commands

import cats.syntax.all._
import io.renku.entities.searchgraphs.PersonInfo
import io.renku.entities.searchgraphs.datasets.Link.{ImportedDataset, OriginalDataset}
import io.renku.entities.searchgraphs.datasets.{DatasetSearchInfo, DatasetSearchInfoOntology, Link, LinkOntology}
import io.renku.graph.model.Schemas.{rdf, renku}
import io.renku.graph.model.datasets
import io.renku.graph.model.entities.Person
import io.renku.graph.model.images.Image
import io.renku.jsonld.Property
import io.renku.jsonld.syntax._
import io.renku.triplesstore.client.model.{Quad, QuadsEncoder, TripleObject}
import io.renku.triplesstore.client.syntax._

private object Encoders {

  implicit val personInfoEncoder: QuadsEncoder[PersonInfo] = QuadsEncoder.instance {
    case PersonInfo(resourceId, name) =>
      Set(
        DatasetsQuad(resourceId, rdf / "type", Person.Ontology.typeClass.id),
        DatasetsQuad(resourceId, Person.Ontology.nameProperty.id, name.asObject)
      )
  }

  implicit val imageEncoder: QuadsEncoder[Image] = QuadsEncoder.instance { case Image(resourceId, uri, position) =>
    Set(
      DatasetsQuad(resourceId, rdf / "type", Image.Ontology.typeClass.id),
      DatasetsQuad(resourceId, Image.Ontology.contentUrlProperty.id, uri.asObject),
      DatasetsQuad(resourceId, Image.Ontology.positionProperty.id, position.asObject)
    )
  }

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

  implicit val searchInfoEncoder: QuadsEncoder[DatasetSearchInfo] = QuadsEncoder.instance { info =>
    def searchInfoQuad(predicate: Property, obj: TripleObject): Quad =
      DatasetsQuad(info.topmostSameAs, predicate, obj)

    val createdOrPublishedQuad = info.createdOrPublished match {
      case d: datasets.DateCreated =>
        searchInfoQuad(DatasetSearchInfoOntology.dateCreatedProperty.id, d.asObject)
      case d: datasets.DatePublished =>
        searchInfoQuad(DatasetSearchInfoOntology.datePublishedProperty.id, d.asObject)
    }

    val maybeDateModifiedQuad = info.maybeDateModified.map { d =>
      searchInfoQuad(DatasetSearchInfoOntology.dateModifiedProperty.id, d.asObject)
    }

    val maybeDescriptionQuad = info.maybeDescription.map { d =>
      searchInfoQuad(DatasetSearchInfoOntology.descriptionProperty.id, d.asObject)
    }

    val creatorsQuads = info.creators.toList.toSet.flatMap { (pi: PersonInfo) =>
      pi.asQuads +
        searchInfoQuad(DatasetSearchInfoOntology.creatorProperty, pi.resourceId.asEntityId)
    }

    val keywordsQuads = info.keywords.toSet.map { (k: datasets.Keyword) =>
      searchInfoQuad(DatasetSearchInfoOntology.keywordsProperty.id, k.asObject)
    }

    val imagesQuads = info.images.toSet.flatMap { (i: Image) =>
      i.asQuads +
        searchInfoQuad(DatasetSearchInfoOntology.imageProperty, i.resourceId.asEntityId)
    }

    val linksQuads = info.links.toList.toSet.flatMap { (l: Link) =>
      l.asQuads +
        searchInfoQuad(DatasetSearchInfoOntology.linkProperty, l.resourceId.asEntityId)
    }

    Set(
      searchInfoQuad(rdf / "type", DatasetSearchInfoOntology.typeDef.clazz.id).some,
      searchInfoQuad(DatasetSearchInfoOntology.slugProperty.id, info.name.asObject).some,
      searchInfoQuad(DatasetSearchInfoOntology.nameProperty.id, info.title.asObject).some,
      searchInfoQuad(DatasetSearchInfoOntology.visibilityProperty.id, info.visibility.asObject).some,
      createdOrPublishedQuad.some,
      maybeDateModifiedQuad,
      maybeDescriptionQuad
    ).flatten ++ creatorsQuads ++ keywordsQuads ++ imagesQuads ++ linksQuads
  }
}
