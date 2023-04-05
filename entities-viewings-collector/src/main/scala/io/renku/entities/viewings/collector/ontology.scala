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

package io.renku.entities.viewings.collector

import io.renku.graph.model.Schemas.{renku, xsd}
import io.renku.graph.model.entities.{Dataset, Project}
import io.renku.jsonld.ontology.{Class, DataProperties, DataProperty, ObjectProperties, ObjectProperty, Type}
import io.renku.jsonld.Property

object ProjectViewedTimeOntology {

  val classType:          Property         = renku / "ProjectViewedTime"
  val dataViewedProperty: DataProperty.Def = DataProperty(renku / "dateViewed", xsd / "dateTime")

  lazy val typeDef: Type = Type.Def(
    Class(classType),
    dataViewedProperty
  )
}

object PersonViewingOntology {

  val classType:             Property = renku / "PersonViewing"
  val viewedProjectProperty: Property = renku / "viewedProject"
  val viewedDatasetProperty: Property = renku / "viewedDataset"

  lazy val typeDef: Type = Type.Def(
    Class(classType),
    ObjectProperty(viewedProjectProperty, PersonViewedProjectOntology.typeDef),
    ObjectProperty(viewedDatasetProperty, PersonViewedDatasetOntology.typeDef)
  )
}

object PersonViewedProjectOntology {

  val classType:          Property         = renku / "ViewedProject"
  val projectProperty:    Property         = renku / "project"
  val dateViewedProperty: DataProperty.Def = ProjectViewedTimeOntology.dataViewedProperty

  lazy val typeDef: Type = Type.Def(
    Class(classType),
    ObjectProperties(
      ObjectProperty(projectProperty, Project.Ontology.typeDef)
    ),
    DataProperties(
      dateViewedProperty
    )
  )
}

object PersonViewedDatasetOntology {

  val classType:          Property         = renku / "ViewedDataset"
  val datasetProperty:    Property         = renku / "dataset"
  val dateViewedProperty: DataProperty.Def = DataProperty(renku / "dateViewed", xsd / "dateTime")

  lazy val typeDef: Type = Type.Def(
    Class(classType),
    ObjectProperties(
      ObjectProperty(datasetProperty, Dataset.Ontology.typeDef)
    ),
    DataProperties(
      dateViewedProperty
    )
  )
}
