/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.projects

import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.Schemas._
import ch.datascience.graph.model.projects.{DateCreated, Name, ResourceId}
import ch.datascience.graph.model.{SchemaVersion, users}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.rdfstore.entities.Project
import ch.datascience.rdfstore.entities.bundles.fileCommit
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import io.circe.{Decoder, Encoder, Json}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityId, JsonLD, Property}
import monocle.function.Plated
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable

class ProjectPropertiesRemoverSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "ProjectPropertiesRemover" should {

    "remove schema:dateCreated, and schema:creator, and schema:name properties from all the Project entities in the given JSON" in {
      forAll { project: Project =>
        val triples = JsonLDTriples {
          JsonLD
            .arr(
              fileCommit()(
                projectPath = project.path,
                projectName = project.name,
                maybeVisibility = project.maybeVisibility,
                projectDateCreated = project.dateCreated,
                maybeProjectCreator = project.maybeCreator,
                maybeParent = project.maybeParentProject,
                projectVersion = project.version
              ),
              project.asJsonLD
            )
            .toJson
        }

        // assume there are names, createdDates and creators initially
        triples.collectAllProjects shouldBe Set(
          TransformedProject(
            project,
            project.name.some,
            project.dateCreated.some,
            project.maybeCreator.asJsonLD.entityId
          ).some,
          project.maybeParentProject.map { parent =>
            TransformedProject(
              parent,
              parent.name.some,
              parent.dateCreated.some,
              parent.maybeCreator.asJsonLD.entityId
            )
          }
        ).flatten

        removeProperties(triples).collectAllProjects shouldBe Set(
          TransformedProject(
            project,
            maybeName = None,
            maybeCreatedDate = None,
            maybeCreatorId = None
          ).some,
          project.maybeParentProject.map { parent =>
            TransformedProject(
              parent,
              maybeName = None,
              maybeCreatedDate = None,
              maybeCreatorId = None
            )
          }
        ).flatten
      }
    }
  }

  private val removeProperties = new ProjectPropertiesRemover()

  private implicit class TriplesOps(triples: JsonLDTriples) {

    lazy val collectAllProjects: Set[TransformedProject] = {
      val collected = mutable.HashSet.empty[TransformedProject]
      Plated.transform[Json] { implicit json =>
        root.`@type`.each.string.getAll(json) match {
          case types if types.contains("http://schema.org/Project") =>
            collected add TransformedProject(
              json.getId[ResourceId].getOrElse(fail("Project '@id' not found")),
              (schema / "name").getValues[Name],
              (schema / "dateCreated").getValues[DateCreated],
              (schema / "creator").getId[users.ResourceId],
              (prov / "wasDerivedFrom").getId[ResourceId],
              (schema / "schemaVersion").getValues[SchemaVersion]
            )
          case _ => ()
        }
        json
      }(triples.value)
      collected.toSet
    }

    private implicit class JsonOps(json: Json) {
      def getId[T](implicit decoder: Decoder[T], encoder: Encoder[T]): Option[T] =
        root.`@id`.as[T].getOption(json)
    }

    private implicit class PropertyOps(property: Property)(implicit json: Json) {

      def getValues[T](implicit decoder: Decoder[T], encoder: Encoder[T]): List[T] =
        root.selectDynamic(property.toString).`@value`.as[T].getOption(json).map(List(_)).getOrElse(Nil) ++
          root.selectDynamic(property.toString).each.`@value`.as[T].getAll(json)

      def getId[T](implicit decoder: Decoder[T], encoder: Encoder[T]): Option[T] =
        root.selectDynamic(property.toString).as[Json].getOption(json).flatMap(_.getId[T])

    }

  }

  case class TransformedProject(id:                 ResourceId,
                                maybeName:          List[Name],
                                maybeDateCreated:   List[DateCreated],
                                maybeCreator:       Option[users.ResourceId],
                                maybeParentProject: Option[ResourceId],
                                version:            List[SchemaVersion]
  )

  object TransformedProject {
    def apply(project:          Project,
              maybeName:        Option[Name],
              maybeCreatedDate: Option[DateCreated],
              maybeCreatorId:   Option[EntityId]
    ): TransformedProject =
      TransformedProject(
        project.asJsonLD.entityId
          .map(id => ResourceId(id))
          .getOrElse(fail("Project's entityId finding problem")),
        maybeName.map(List(_)).getOrElse(Nil),
        maybeCreatedDate.map(List(_)).getOrElse(Nil),
        maybeCreatorId.map(id => users.ResourceId(id)),
        project.maybeParentProject.flatMap(_.asJsonLD.entityId).map(id => ResourceId(id)),
        List(SchemaVersion(project.version.toString))
      )
  }

}
