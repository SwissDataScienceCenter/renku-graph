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

package io.renku.triplesgenerator.tsprovisioning.triplesuploading

import cats.data.EitherT.rightT
import cats.syntax.all._
import io.renku.generators.CommonGraphGenerators.sparqlQueries
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.entities.EntityFunctions
import io.renku.graph.model.testentities._
import io.renku.graph.model.{GraphClass, entities}
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLDEncoder, NamedGraph}
import io.renku.testtools.IOSpec
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Try

class TransformationResultsUploaderSpec extends AnyWordSpec with MockFactory with should.Matchers with IOSpec {

  "execute" should {

    "call the queryRunner's run with the given query" in new TestCase {
      val query = sparqlQueries.generateOne

      (queryRunner.run _).expects(query).returning(rightT(()))

      (uploader execute query).value shouldBe ().asRight.pure[Try]
    }
  }

  "upload" should {

    "encode the given project to JsonLD, flatten it, " +
      "separate Person and non-Person entities to their own NamedGraphs " +
      "and pass them both to the projectUploader" in new TestCase {

        val project = anyProjectEntities.generateOne.to[entities.Project]

        implicit val projectEncoder: JsonLDEncoder[entities.Project] =
          EntityFunctions[entities.Project].encoder(GraphClass.Project)
        val projectGraph = NamedGraph
          .fromJsonLDsUnsafe(project.resourceId.asEntityId, project.asJsonLD)
          .flatten
          .fold(fail(_), identity)
        (jsonLDUploader.uploadJsonLD _)
          .expects(projectGraph)
          .returning(rightT(()))

        EntityFunctions[entities.Project].findAllPersons(project).toList match {
          case Nil => ()
          case h :: t =>
            implicit val encoder: JsonLDEncoder[entities.Person] =
              EntityFunctions[entities.Person].encoder(GraphClass.Persons)
            val personsGraph = NamedGraph
              .fromJsonLDsUnsafe(schema / "Person", h.asJsonLD, t.map(_.asJsonLD): _*)
              .flatten
              .fold(fail(_), identity)
            (jsonLDUploader.uploadJsonLD _)
              .expects(personsGraph)
              .returning(rightT(()))
        }

        (uploader upload project).value shouldBe ().asRight.pure[Try]
      }
  }

  private trait TestCase {
    val jsonLDUploader = mock[JsonLDUploader[Try]]
    val queryRunner    = mock[UpdateQueryRunner[Try]]
    val uploader       = new TransformationResultsUploaderImpl(jsonLDUploader, queryRunner)
  }
}
