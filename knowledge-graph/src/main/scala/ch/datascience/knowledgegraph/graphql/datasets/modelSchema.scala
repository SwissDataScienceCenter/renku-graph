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
    description = "DataSet",
    fields = fields[Unit, DataSet](
      Field("identifier", StringType, Some("DataSet id"), resolve = _.value.id.toString),
      Field("name", StringType, Some("DataSet name"), resolve     = _.value.name.toString),
      Field("description",
            OptionType(StringType),
            Some("DataSet description"),
            resolve                                                                          = _.value.maybeDescription.map(_.toString)),
      Field("created", createdType, Some("DataSet creation info"), resolve                   = _.value.created),
      Field("published", OptionType(publishedType), Some("DataSet publishing info"), resolve = _.value.maybePublished)
    )
  )

  private lazy val createdType: ObjectType[Unit, DataSetCreation] = ObjectType[Unit, DataSetCreation](
    name        = "dataSetCreation",
    description = "DataSetCreation",
    fields = fields[Unit, DataSetCreation](
      Field("dateCreated", StringType, Some("DataSet creation date"), resolve = _.value.date.toString),
      Field("agent", dataSetCreatorType, Some("DataSet creator"), resolve     = _.value.agent)
    )
  )

  private lazy val dataSetCreatorType: ObjectType[Unit, DataSetAgent] = ObjectType[Unit, DataSetAgent](
    name        = "dataSetCreator",
    description = "DataSetCreator",
    fields = fields[Unit, DataSetAgent](
      Field("email", StringType, Some("DataSet creator email"), resolve = _.value.email.toString),
      Field("name", StringType, Some("DataSet creator name"), resolve   = _.value.name.toString)
    )
  )

  private lazy val publishedType: ObjectType[Unit, DataSetPublishing] = ObjectType[Unit, DataSetPublishing](
    name        = "dataSetPublishing",
    description = "DataSetPublishing",
    fields = fields[Unit, DataSetPublishing](
      Field("datePublished", StringType, Some("DataSet publishing date"), resolve = _.value.date.toString)
    )
  )
}
