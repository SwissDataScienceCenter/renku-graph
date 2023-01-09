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

import io.renku.graph.model.Schemas.{xsd, _}
import io.renku.graph.model.entities.{Dataset, Person, Project}
import io.renku.graph.model.images.Image
import io.renku.jsonld.Property
import io.renku.jsonld.ontology._

object SearchInfoOntology {

  val nameProperty:          DataProperty.Def = Dataset.Ontology.nameProperty
  val visibilityProperty:    DataProperty.Def = Project.Ontology.visibilityProperty
  val dateCreatedProperty:   DataProperty.Def = Dataset.Ontology.dateCreatedProperty
  val datePublishedProperty: DataProperty.Def = Dataset.Ontology.datePublishedProperty
  val dateModifiedProperty:  DataProperty.Def = DataProperty(schema / "dateModified", xsd / "dateTime")
  val keywordsProperty:      DataProperty.Def = Dataset.Ontology.keywordsProperty
  val descriptionProperty:   DataProperty.Def = Dataset.Ontology.descriptionProperty
  val creatorProperty:       Property         = Dataset.Ontology.creator
  val imageProperty:         Property         = Dataset.Ontology.image
  val linkProperty:          Property         = renku / "datasetProjectLink"

  lazy val typeDef: Type = Type.Def(
    Class(renku / "DiscoverableDataset"),
    ObjectProperties(
      ObjectProperty(creatorProperty, PersonInfoOntology.typeDef),
      ObjectProperty(imageProperty, Image.Ontology.typeDef),
      ObjectProperty(linkProperty, LinkOntology.typeDef)
    ),
    DataProperties(
      nameProperty,
      visibilityProperty,
      dateCreatedProperty,
      datePublishedProperty,
      dateModifiedProperty,
      keywordsProperty,
      descriptionProperty
    )
  )
}

object PersonInfoOntology {

  val nameProperty: DataProperty.Def = Person.Ontology.nameProperty

  lazy val typeDef: Type = Type.Def(
    Class(renku / "DiscoverableDatasetPerson"),
    ObjectProperties(),
    DataProperties(nameProperty)
  )
}

object LinkOntology {

  val project: Property = renku / "project"
  val dataset: Property = renku / "dataset"

  lazy val typeDef: Type = Type.Def(
    Class(renku / "DatasetProjectLink"),
    ObjectProperties(
      ObjectProperty(project, Project.Ontology.typeDef),
      ObjectProperty(dataset, Dataset.Ontology.typeDef)
    ),
    DataProperties()
  )
}
