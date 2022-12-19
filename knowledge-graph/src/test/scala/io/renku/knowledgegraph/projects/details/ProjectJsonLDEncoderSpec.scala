/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph.projects.details

import ProjectsGenerators._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas._
import io.renku.graph.model._
import io.renku.jsonld.JsonLDDecoder
import model.Project.DateUpdated
import model._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectJsonLDEncoderSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "encode" should {

    "convert the model.Project object to Json" in {
      forAll { project: Project =>
        (ProjectJsonLDEncoder encode project).cursor.as(decoder(project)) shouldBe project.asRight
      }
    }
  }

  private def decoder(project: Project): JsonLDDecoder[Project] = JsonLDDecoder.entity(entities.Project.entityTypes) {
    cursor =>
      for {
        resourceId   <- cursor.downEntityId.as[projects.ResourceId]
        identifier   <- cursor.downField(schema / "identifier").as[projects.GitLabId]
        path         <- cursor.downField(renku / "projectPath").as[projects.Path]
        name         <- cursor.downField(schema / "name").as[projects.Name]
        maybeDesc    <- cursor.downField(schema / "description").as[Option[projects.Description]]
        visibility   <- cursor.downField(renku / "projectVisibility").as[projects.Visibility]
        dateCreated  <- cursor.downField(schema / "dateCreated").as[projects.DateCreated]
        maybeCreator <- cursor.downField(schema / "creator").as[Option[Creator]]
        dateUpdated  <- cursor.downField(schema / "dateModified").as[DateUpdated]
        maybeParent  <- cursor.downField(prov / "wasDerivedFrom").as[Option[ParentProject]]
        keywords     <- cursor.downField(schema / "keywords").as[List[projects.Keyword]]
        maybeVersion <- cursor.downField(schema / "schemaVersion").as[Option[SchemaVersion]]
      } yield Project(
        resourceId,
        identifier,
        path,
        name,
        maybeDesc,
        visibility,
        Creation(dateCreated, maybeCreator),
        dateUpdated,
        project.urls,
        Forking(project.forking.forksCount, maybeParent),
        keywords.toSet,
        project.starsCount,
        project.permissions,
        project.statistics,
        maybeVersion
      )
  }

  private implicit lazy val creatorDecoder: JsonLDDecoder[Creator] =
    JsonLDDecoder.entity(entities.Person.entityTypes) { cursor =>
      for {
        resourceId       <- cursor.downEntityId.as[persons.ResourceId]
        name             <- cursor.downField(schema / "name").as[persons.Name]
        maybeEmail       <- cursor.downField(schema / "email").as[Option[persons.Email]]
        maybeAffiliation <- cursor.downField(schema / "affiliation").as[Option[persons.Affiliation]]
      } yield Creator(resourceId, name, maybeEmail, maybeAffiliation)
    }

  private implicit lazy val parentDecoder: JsonLDDecoder[ParentProject] =
    JsonLDDecoder.entity(entities.Project.entityTypes) { cursor =>
      for {
        resourceId   <- cursor.downEntityId.as[projects.ResourceId]
        path         <- cursor.downField(renku / "projectPath").as[projects.Path]
        name         <- cursor.downField(schema / "name").as[projects.Name]
        dateCreated  <- cursor.downField(schema / "dateCreated").as[projects.DateCreated]
        maybeCreator <- cursor.downField(schema / "creator").as[Option[Creator]]
      } yield ParentProject(resourceId, path, name, Creation(dateCreated, maybeCreator))
    }
}
