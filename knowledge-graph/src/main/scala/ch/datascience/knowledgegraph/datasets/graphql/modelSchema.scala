/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.knowledgegraph.datasets.graphql

import ch.datascience.knowledgegraph.datasets.model._
import sangria.schema._

object modelSchema {

  val datasetType: ObjectType[Unit, Dataset] = ObjectType[Unit, Dataset](
    name        = "dataset",
    description = "Data-set info",
    fields = fields[Unit, Dataset](
      Field("identifier", StringType, Some("Data-set identifier"), resolve = _.value.id.toString),
      Field("name", StringType, Some("Data-set name"), resolve             = _.value.name.toString),
      Field("description",
            OptionType(StringType),
            Some("Data-set description"),
            resolve                                                                                 = _.value.maybeDescription.map(_.toString)),
      Field("created", createdType, Some("Data-set creation info"), resolve                         = _.value.created),
      Field("published", publishedType, Some("Data-set publishing info"), resolve                   = _.value.published),
      Field("hasPart", ListType(partType), Some("Data-set files"), resolve                          = _.value.part),
      Field("isPartOf", ListType(projectType), Some("Projects where this dataset is used"), resolve = _.value.project)
    )
  )

  private lazy val createdType: ObjectType[Unit, DatasetCreation] = ObjectType[Unit, DatasetCreation](
    name        = "datasetCreation",
    description = "Data-set creation info",
    fields = fields[Unit, DatasetCreation](
      Field("dateCreated", StringType, Some("Data-set creation date"), resolve             = _.value.date.toString),
      Field("agent", agentType, Some("A person who created the dataset in Renku"), resolve = _.value.agent)
    )
  )

  private lazy val agentType: ObjectType[Unit, DatasetAgent] = ObjectType[Unit, DatasetAgent](
    name        = "datasetAgent",
    description = "A person who created the dataset in Renku",
    fields = fields[Unit, DatasetAgent](
      Field("email", StringType, Some("Dataset agent email"), resolve = _.value.email.toString),
      Field("name", StringType, Some("Dataset agent name"), resolve   = _.value.name.toString)
    )
  )

  private lazy val publishedType: ObjectType[Unit, DatasetPublishing] = ObjectType[Unit, DatasetPublishing](
    name        = "datasetPublishing",
    description = "Data-set publishing info",
    fields = fields[Unit, DatasetPublishing](
      Field("datePublished",
            OptionType(StringType),
            Some("Data-set publishing date"),
            resolve                                                                       = _.value.maybeDate.map(_.toString)),
      Field("creator", ListType(creatorType), Some("A list of dataset creators"), resolve = _.value.creators.toList)
    )
  )

  private lazy val creatorType: ObjectType[Unit, DatasetCreator] = ObjectType[Unit, DatasetCreator](
    name        = "datasetCreator",
    description = "An author of imported dataset or dataset creator if originating in Renku",
    fields = fields[Unit, DatasetCreator](
      Field("email",
            OptionType(StringType),
            Some("Dataset creator email"),
            resolve                                                   = _.value.maybeEmail.map(_.toString)),
      Field("name", StringType, Some("Dataset creator name"), resolve = _.value.name.toString)
    )
  )

  private lazy val partType: ObjectType[Unit, DatasetPart] = ObjectType[Unit, DatasetPart](
    name        = "datasetPart",
    description = "The datasets files",
    fields = fields[Unit, DatasetPart](
      Field("name", StringType, Some("Dataset part name"), resolve           = _.value.name.toString),
      Field("atLocation", StringType, Some("Dataset part location"), resolve = _.value.atLocation.toString)
    )
  )

  private lazy val projectType: ObjectType[Unit, DatasetProject] = ObjectType[Unit, DatasetProject](
    name        = "datasetProject",
    description = "A project where this dataset is used",
    fields = fields[Unit, DatasetProject](
      Field("name", StringType, Some("Project name"), resolve = _.value.name.toString)
    )
  )
}
