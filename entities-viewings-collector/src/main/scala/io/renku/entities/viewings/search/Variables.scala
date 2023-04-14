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

package io.renku.entities.viewings.search

import cats.syntax.all._
import io.circe.Decoder
import io.renku.entities.search.DecodingTools.{toListOf, toListOfImageUris}
import io.renku.entities.search.model.{MatchingScore, Entity => SearchEntity}
import io.renku.graph.model.{persons, projects}
import io.renku.tinytypes.json.TinyTypeDecoders._
import io.renku.triplesstore.ResultsDecoder.read
import io.renku.triplesstore.client.sparql.VarName

/** Top level variables returned from the query */
private object Variables {

  val matchingScore = VarName("matchingScore")
  val entityType    = VarName("entityType")
  val projectId     = VarName("projectId")
  val projectName   = VarName("projectName")
  val projectPath   = VarName("projectPath")
  val visibility    = VarName("visibility")
  val creatorName   = VarName("creatorName")
  val dateCreated   = VarName("dateCreated")
  val description   = VarName("description")
  val keywords      = VarName("keywords")
  val images        = VarName("images")
  val viewedDate    = VarName("viewedDate")

  val all: List[VarName] = List(
    matchingScore,
    entityType,
    projectId,
    projectName,
    projectPath,
    visibility,
    creatorName,
    dateCreated,
    description,
    viewedDate,
    keywords,
    images
  )

  def datasetDecoder: Decoder[SearchEntity.Dataset] = { implicit cursor =>
    ???
  }

  def projectDecoder: Decoder[SearchEntity.Project] = { implicit cursor =>
    for {
      matchingScore    <- read[MatchingScore](matchingScore)
      path             <- read[projects.Path](projectPath)
      name             <- read[projects.Name](projectName)
      visibility       <- read[projects.Visibility](visibility)
      dateCreated      <- read[projects.DateCreated](dateCreated)
      maybeCreatorName <- read[Option[persons.Name]](creatorName)
      keywords <-
        read[Option[String]](keywords) >>= toListOf[projects.Keyword, projects.Keyword.type](projects.Keyword)
      maybeDescription <- read[Option[projects.Description]](description)
      images           <- read[Option[String]](images) >>= toListOfImageUris
    } yield SearchEntity.Project(
      matchingScore,
      path,
      name,
      visibility,
      dateCreated,
      maybeCreatorName,
      keywords,
      maybeDescription,
      images
    )
  }
}
