/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.testentities._
import io.renku.jsonld.syntax._
import org.scalacheck.Gen

private object Generators {

  implicit lazy val projectSearchInfoObjects: Gen[ProjectSearchInfo] = for {
    id           <- projectResourceIds
    name         <- projectNames
    slug         <- projectSlugs
    createdDate  <- projectCreatedDates()
    modifiedDate <- projectModifiedDates(createdDate.value)
    visibility   <- projectVisibilities
    maybeCreator <- personResourceIds.toGeneratorOfOptions
    keywords     <- projectKeywords.toGeneratorOfList(max = 2)
    maybeDesc    <- projectDescriptions.toGeneratorOfOptions
    images       <- imageUris.toGeneratorOfList(max = 2).map(convertImageUris(id.asEntityId))
  } yield ProjectSearchInfo(id,
                            name,
                            slug,
                            visibility,
                            createdDate,
                            modifiedDate,
                            maybeCreator,
                            keywords,
                            maybeDesc,
                            images
  )
}
