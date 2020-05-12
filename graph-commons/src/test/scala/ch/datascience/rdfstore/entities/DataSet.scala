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

package ch.datascience.rdfstore.entities

import ch.datascience.graph.model.datasets._
import ch.datascience.rdfstore.FusekiBaseUrl

final case class DataSet private (id:                  Identifier,
                                  name:                Name,
                                  url:                 Url,
                                  maybeSameAs:         Option[SameAs],
                                  maybeWasDerivedFrom: Option[DerivedFrom],
                                  maybeDescription:    Option[Description],
                                  maybePublishedDate:  Option[PublishedDate],
                                  createdDate:         DateCreated,
                                  creators:            Set[Person],
                                  parts:               List[DataSetPart],
                                  generation:          Generation,
                                  project:             Project,
                                  keywords:            List[Keyword])

object DataSet {

  def nonModified(id:                 Identifier,
                  name:               Name,
                  url:                Url,
                  maybeSameAs:        Option[SameAs],
                  maybeDescription:   Option[Description] = None,
                  maybePublishedDate: Option[PublishedDate] = None,
                  createdDate:        DateCreated,
                  creators:           Set[Person],
                  parts:              List[DataSetPart],
                  generation:         Generation,
                  project:            Project,
                  keywords:           List[Keyword] = Nil): DataSet = DataSet(
    id,
    name,
    url,
    maybeSameAs,
    maybeWasDerivedFrom = None,
    maybeDescription,
    maybePublishedDate,
    createdDate,
    creators,
    parts,
    generation,
    project,
    keywords
  )

  def modified(id:                 Identifier,
               name:               Name,
               url:                Url,
               wasDerivedFrom:     DerivedFrom,
               maybeDescription:   Option[Description] = None,
               maybePublishedDate: Option[PublishedDate] = None,
               createdDate:        DateCreated,
               creators:           Set[Person],
               parts:              List[DataSetPart],
               generation:         Generation,
               project:            Project,
               keywords:           List[Keyword] = Nil): DataSet = DataSet(
    id,
    name,
    url,
    maybeSameAs         = None,
    maybeWasDerivedFrom = Some(wasDerivedFrom),
    maybeDescription,
    maybePublishedDate,
    createdDate,
    creators,
    parts,
    generation,
    project,
    keywords
  )

  import ch.datascience.graph.config.RenkuBaseUrl
  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  def entityId(identifier: Identifier)(implicit renkuBaseUrl: RenkuBaseUrl): EntityId =
    EntityId of (renkuBaseUrl / "datasets" / identifier)

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, fusekiBaseUrl: FusekiBaseUrl): JsonLDEncoder[DataSet] = {
    implicit val creatorsOrdering: Ordering[Person] = Ordering.by((p: Person) => p.name.value)

    JsonLDEncoder.instance { entity =>
      JsonLD.entity(
        entityId(entity.id),
        EntityTypes of (prov / "Entity", wfprov / "Artifact", schema / "Dataset"),
        prov / "atLocation"          -> entity.generation.filePath.asJsonLD,
        prov / "qualifiedGeneration" -> entity.generation.asJsonLD,
        rdfs / "label"               -> entity.id.asJsonLD,
        schema / "identifier"        -> entity.id.asJsonLD,
        schema / "name"              -> entity.name.asJsonLD,
        schema / "url"               -> entity.url.asJsonLD,
        schema / "sameAs"            -> entity.maybeSameAs.asJsonLD,
        schema / "description"       -> entity.maybeDescription.asJsonLD,
        schema / "datePublished"     -> entity.maybePublishedDate.asJsonLD,
        schema / "dateCreated"       -> entity.createdDate.asJsonLD,
        schema / "creator"           -> entity.creators.asJsonLD,
        schema / "hasPart"           -> entity.parts.asJsonLD,
        schema / "keywords"          -> entity.keywords.asJsonLD,
        schema / "isPartOf"          -> entity.project.asJsonLD
      )
    }
  }
}
