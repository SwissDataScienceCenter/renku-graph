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
import ch.datascience.graph.model.projects.{DateCreated, Name, ResourceId}
import ch.datascience.graph.model.{SchemaVersion, users}
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.rdfstore.entities.Project.ForksCount
import ch.datascience.rdfstore.entities.{Project, _}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import io.circe.{Decoder, Encoder, Json}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLD, Property}
import monocle.function.Plated
import org.scalacheck.Gen
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable

class ProjectPropertiesRemoverSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "ProjectPropertiesRemover" should {

    "remove schema:dateCreated, and schema:creator, and schema:name properties from all the Project entities in the given JSON" in {
      forAll(
        Gen.oneOf(projectEntities[ForksCount.Zero](visibilityAny),
                  projectEntities[ForksCount.Zero](visibilityAny).map(_.forkOnce()._2)
        )
      ) { project: Project[ForksCount.Zero] =>
        val triples = JsonLDTriples(JsonLD.arr(project.asJsonLD).toJson)

        // assume there are names, createdDates and creators initially

        triples.collectAllProjects shouldBe {
          project match {
            case project: ProjectWithParent[_] =>
              Set(project.to[TransformedProject], project.parent.to[TransformedProject])
            case _ => Set(project.to[TransformedProject])
          }
        }

        removeProperties(triples).collectAllProjects shouldBe (project match {
          case project: ProjectWithParent[_] =>
            Set(
              TransformedProject(
                project.resourceId,
                maybeName = Nil,
                maybeDateCreated = Nil,
                maybeCreator = None,
                maybeParentProject = project.parent.resourceId.some,
                version = List(project.version)
              ),
              TransformedProject(
                project.parent.resourceId,
                maybeName = Nil,
                maybeDateCreated = Nil,
                maybeCreator = None,
                maybeParentProject = None,
                version = List(project.parent.version)
              )
            )
          case _ =>
            Set(
              TransformedProject(
                project.resourceId,
                maybeName = Nil,
                maybeDateCreated = Nil,
                maybeCreator = None,
                maybeParentProject = None,
                version = List(project.version)
              )
            )
        })
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

    implicit lazy val converter: Project[ForksCount] => TransformedProject = {
      case project: ProjectWithParent[ForksCount] =>
        TransformedProject(
          project.resourceId,
          List(project.name),
          List(project.dateCreated),
          project.maybeCreator.map(creator => creator.resourceId),
          project.parent.resourceId.some,
          List(SchemaVersion(project.version.toString))
        )
      case project: Project[ForksCount] =>
        TransformedProject(
          project.resourceId,
          List(project.name),
          List(project.dateCreated),
          project.maybeCreator.map(creator => creator.resourceId),
          None,
          List(SchemaVersion(project.version.toString))
        )
    }
  }
}
