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

import ch.datascience.generators.Generators._
import ch.datascience.knowledgegraph.graphql.datasets.model.{DataSet, DataSetName}
import org.scalacheck.Gen

object DataSetsGenerators {

  implicit val dataSetName: Gen[DataSetName] = nonEmptyStrings() map DataSetName.apply
  implicit val dataSets: Gen[DataSet] = for {
    name <- dataSetName
  } yield DataSet(name)
}
