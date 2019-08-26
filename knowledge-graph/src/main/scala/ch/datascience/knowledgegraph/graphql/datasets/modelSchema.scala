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

package ch.datascience.knowledgegraph.graphql.datasets

import model._
import sangria.schema._

object modelSchema {

  val dataSetType: ObjectType[Unit, DataSet] = ObjectType[Unit, DataSet](
    name        = "dataSet",
    description = "Data-set info",
    fields = fields[Unit, DataSet](
      Field("identifier", StringType, Some("Data-set identifier"), resolve = _.value.id.toString),
      Field("name", StringType, Some("Data-set name"), resolve             = _.value.name.toString),
      Field("description",
            OptionType(StringType),
            Some("Data-set description"),
            resolve                                                               = _.value.maybeDescription.map(_.toString)),
      Field("created", createdType, Some("Data-set creation info"), resolve       = _.value.created),
      Field("published", publishedType, Some("Data-set publishing info"), resolve = _.value.published),
      Field("hasPart", ListType(partType), Some("Data-set files"), resolve        = _.value.part)
    )
  )

  private lazy val createdType: ObjectType[Unit, DataSetCreation] = ObjectType[Unit, DataSetCreation](
    name        = "dataSetCreation",
    description = "Data-set creation info",
    fields = fields[Unit, DataSetCreation](
      Field("dateCreated", StringType, Some("Data-set creation date"), resolve                     = _.value.date.toString),
      Field("agent", dataSetAgentType, Some("A person who created the data-set in Renku"), resolve = _.value.agent)
    )
  )

  private lazy val dataSetAgentType: ObjectType[Unit, DataSetAgent] = ObjectType[Unit, DataSetAgent](
    name        = "dataSetAgent",
    description = "A person who created the data-set in Renku",
    fields = fields[Unit, DataSetAgent](
      Field("email", StringType, Some("DataSet agent email"), resolve = _.value.email.toString),
      Field("name", StringType, Some("DataSet agent name"), resolve   = _.value.name.toString)
    )
  )

  private lazy val publishedType: ObjectType[Unit, DataSetPublishing] = ObjectType[Unit, DataSetPublishing](
    name        = "dataSetPublishing",
    description = "Data-set publishing info",
    fields = fields[Unit, DataSetPublishing](
      Field("datePublished",
            OptionType(StringType),
            Some("Data-set publishing date"),
            resolve = _.value.maybeDate.map(_.toString)),
      Field("creator",
            ListType(dataSetCreatorType),
            Some("A list of data-set creators"),
            resolve = _.value.creators.toList)
    )
  )

  private lazy val dataSetCreatorType: ObjectType[Unit, DataSetCreator] = ObjectType[Unit, DataSetCreator](
    name        = "dataSetCreator",
    description = "An author of imported data-set or data-set creator if originating in Renku",
    fields = fields[Unit, DataSetCreator](
      Field("email",
            OptionType(StringType),
            Some("DataSet creator email"),
            resolve                                                   = _.value.maybeEmail.map(_.toString)),
      Field("name", StringType, Some("DataSet creator name"), resolve = _.value.name.toString)
    )
  )

  private lazy val partType: ObjectType[Unit, DataSetPart] = ObjectType[Unit, DataSetPart](
    name        = "dataSetPart",
    description = "The data-sets files",
    fields = fields[Unit, DataSetPart](
      Field("name", StringType, Some("DataSet part name"), resolve                  = _.value.name.toString),
      Field("atLocation", StringType, Some("DataSet part location"), resolve        = _.value.atLocation.toString),
      Field("dateCreated", StringType, Some("Data-set part creation date"), resolve = _.value.dateCreated.toString),
    )
  )
}
