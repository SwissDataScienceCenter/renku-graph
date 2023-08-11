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

import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.entities.{Dataset, Person, Project}
import io.renku.graph.model.images.Image
import io.renku.jsonld.Property
import io.renku.jsonld.ontology._

object ProjectSearchInfoOntology {

  val nameProperty:         DataProperty.Def = Project.Ontology.nameProperty
  val slugProperty:         DataProperty.Def = Project.Ontology.slugProperty
  val pathProperty:         DataProperty.Def = Project.Ontology.pathProperty
  val visibilityProperty:   DataProperty.Def = Project.Ontology.visibilityProperty
  val dateCreatedProperty:  DataProperty.Def = Project.Ontology.dateCreatedProperty
  val dateModifiedProperty: DataProperty.Def = Project.Ontology.dateModifiedProperty
  val keywordsProperty:     DataProperty.Def = Project.Ontology.keywordsProperty
  val descriptionProperty:  DataProperty.Def = Project.Ontology.descriptionProperty
  val creatorProperty:      Property         = Project.Ontology.creator
  val imageProperty:        Property         = Project.Ontology.image

  lazy val typeDef: Type = Type.Def(
    Class(renku / "DiscoverableProject"),
    ObjectProperties(
      ObjectProperty(creatorProperty, Person.Ontology.typeDef),
      ObjectProperty(imageProperty, Image.Ontology.typeDef)
    ),
    DataProperties(
      nameProperty,
      slugProperty,
      pathProperty,
      visibilityProperty,
      dateCreatedProperty,
      dateModifiedProperty,
      keywordsProperty,
      descriptionProperty
    )
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
