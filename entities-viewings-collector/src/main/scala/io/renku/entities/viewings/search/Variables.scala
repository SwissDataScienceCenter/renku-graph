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
import io.circe.{Decoder, DecodingFailure}
import io.renku.entities.search.DecodingTools.{toListOf, toListOfImageUris}
import io.renku.entities.search.model.{MatchingScore, Entity => SearchEntity}
import io.renku.graph.model.{datasets, persons, projects}
import io.renku.tinytypes.json.TinyTypeDecoders._
import io.renku.triplesstore.ResultsDecoder.read
import io.renku.triplesstore.client.sparql.VarName

/** Top level variables returned from the query */
object Variables {

  val viewedDate = VarName("viewedDate")

  object Dataset {

    val matchingScore     = VarName("matchingScore")
    val entityType        = VarName("entityType")
    val datasetName       = VarName("datasetName")
    val datasetSlug       = VarName("datasetSlug")
    val datasetSameAs     = VarName("datasetSameAs")
    val dateCreated       = VarName("dateCreated")
    val datePublished     = VarName("datePublished")
    val dateModified      = VarName("dateModified")
    val date              = VarName("date")
    val creatorNames      = VarName("creatorNames")
    val description       = VarName("description")
    val keywords          = VarName("keywords")
    val images            = VarName("images")
    val projectSlug       = VarName("projectSlug")
    val projectVisibility = VarName("projectVisibility")
    lazy val viewedDate   = Variables.viewedDate

    lazy val all: List[VarName] = List(
      matchingScore,
      entityType,
      datasetName,
      datasetSlug,
      datasetSameAs,
      dateCreated,
      datePublished,
      dateModified,
      date,
      creatorNames,
      description,
      projectSlug,
      projectVisibility,
      keywords,
      images,
      viewedDate
    )

    def decoder: Decoder[SearchEntity.Dataset] = { implicit cursor =>
      for {
        matchingScore      <- read[MatchingScore](matchingScore)
        name               <- read[datasets.Name](datasetName)
        slug               <- read[datasets.Slug](datasetSlug)
        sameAs             <- read[datasets.TopmostSameAs](datasetSameAs)
        projectSlug        <- read[projects.Slug](projectSlug)
        visibility         <- read[projects.Visibility](projectVisibility)
        maybeDateCreated   <- read[Option[datasets.DateCreated]](dateCreated)
        maybeDatePublished <- read[Option[datasets.DatePublished]](datePublished)
        maybeDateModified  <- read[Option[datasets.DateCreated]](dateModified)
        date <-
          Either.fromOption(maybeDateModified.orElse(maybeDateCreated.orElse(maybeDatePublished)),
                            ifNone = DecodingFailure("No dataset date", Nil)
          )
        creators <- read[Option[String]](creatorNames) >>= toListOf[persons.Name, persons.Name.type](persons.Name)
        keywords <-
          read[Option[String]](keywords) >>= toListOf[datasets.Keyword, datasets.Keyword.type](datasets.Keyword)
        maybeDesc <- read[Option[datasets.Description]](description)
        images    <- read[Option[String]](images) >>= toListOfImageUris
      } yield SearchEntity.Dataset(
        matchingScore,
        sameAs,
        slug,
        name,
        visibility,
        date,
        maybeDateModified.map(d => datasets.DateModified(d.value)),
        creators,
        keywords,
        maybeDesc,
        images,
        projectSlug
      )
    }
  }

  object Project {

    val matchingScore   = VarName("matchingScore")
    val entityType      = VarName("entityType")
    val projectName     = VarName("projectName")
    val projectSlug     = VarName("projectSlug")
    val visibility      = VarName("visibility")
    val creatorNames    = VarName("creatorNames")
    val dateCreated     = VarName("dateCreated")
    val dateModified    = VarName("dateModified")
    val description     = VarName("description")
    val keywords        = VarName("keywords")
    val images          = VarName("images")
    lazy val viewedDate = Variables.viewedDate

    lazy val all: List[VarName] = List(
      matchingScore,
      entityType,
      projectName,
      projectSlug,
      visibility,
      creatorNames,
      dateCreated,
      dateModified,
      description,
      viewedDate,
      keywords,
      images
    )

    def decoder: Decoder[SearchEntity.Project] = { implicit cursor =>
      for {
        matchingScore    <- read[MatchingScore](matchingScore)
        name             <- read[projects.Name](projectName)
        slug             <- read[projects.Slug](projectSlug)
        visibility       <- read[projects.Visibility](visibility)
        dateCreated      <- read[projects.DateCreated](dateCreated)
        dateModified     <- read[projects.DateModified](dateModified)
        maybeCreatorName <- read[Option[persons.Name]](creatorNames)
        keywords <-
          read[Option[String]](keywords) >>= toListOf[projects.Keyword, projects.Keyword.type](projects.Keyword)
        maybeDescription <- read[Option[projects.Description]](description)
        images           <- read[Option[String]](images) >>= toListOfImageUris
      } yield SearchEntity.Project(
        matchingScore,
        slug,
        name,
        visibility,
        dateCreated,
        dateModified,
        maybeCreatorName,
        keywords,
        maybeDescription,
        images
      )
    }
  }

  val all: List[VarName] =
    (Project.all.toSet ++ Dataset.all.toSet).toList
}
