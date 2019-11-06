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

package ch.datascience.triplesgenerator.reprovisioning.postreprovisioning

import cats.effect.IO
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.users.Name
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.rdfstore.triples.entities.Person
import io.circe.literal._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class DuplicatePersonNameRemoverSpec extends WordSpec with InMemoryRdfStore {

  "run" should {

    "do nothing if there's only single name on Person entities" in new TestCase {

      val name = names.generateOne
      loadToStore {
        triples(
          List(
            Person(Person.Id(name), Some(emails.generateOne))
          )
        )
      }

      val initialStoreSize = rdfStoreSize

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      rdfStoreSize shouldBe initialStoreSize

      leftSchemaNames shouldBe Set(name.toString)
    }

    "do nothing if there's only single name on Person entities even if it does not match the 'valid name pattern'" in new TestCase {

      val name = (nonEmptyStrings() -> nonEmptyStrings()).map { case (first, second) => s"$first-$second" }.generateOne
      loadToStore {
        triples(
          List(
            Person(Person.Id(Name(name)), Some(emails.generateOne))
          )
        )
      }

      val initialStoreSize = rdfStoreSize

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      rdfStoreSize shouldBe initialStoreSize

      leftSchemaNames shouldBe Set(name)
    }

    "remove duplicate Person schema:name triples not matching the 'valid name pattern'" in new TestCase {

      val name       = names.generateOne
      val otherName1 = s"${nonEmptyStrings().generateOne}-${nonEmptyStrings().generateOne}"
      val otherName2 = names.generateOne
      val personId   = Person.Id(name)

      loadToStore {
        triples(
          List(
            Person(personId, None) deepMerge {
              json"""{
                "schema:name": [${name.value}, $otherName1, ${otherName2.value}],
                "rdfs:label": [${name.value}, $otherName1, ${otherName2.value}]
              }"""
            }
          )
        )
      }

      leftSchemaNames should contain theSameElementsAs List(name.toString, otherName1, otherName2.value)

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      leftSchemaNames should contain oneOf (name.toString, otherName2.value)
    }

    "remove duplicate Person schema:name triples even if all are not matching the 'valid name pattern'" in new TestCase {

      val otherName1 = s"${nonEmptyStrings().generateOne}-${nonEmptyStrings().generateOne}"
      val otherName2 = s"${nonEmptyStrings().generateOne}-${nonEmptyStrings().generateOne}"
      loadToStore {
        triples(
          List(
            Person(Person.Id(names.generateOne), None) deepMerge {
              json"""{
                "schema:name": [$otherName1, $otherName2],
                "rdfs:label": [$otherName1, $otherName2]
              }"""
            }
          )
        )
      }

      leftSchemaNames should contain theSameElementsAs List(otherName1, otherName2)

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      leftSchemaNames should contain oneOf (otherName1, otherName2)
    }

    "remove some duplicate Person schema:name if all match the 'valid name pattern'" in new TestCase {

      val name       = names.generateOne
      val otherName1 = names.generateOne
      val otherName2 = names.generateOne
      val personId   = Person.Id(name)

      loadToStore {
        triples(
          List(
            Person(personId, None) deepMerge {
              json"""{
                "schema:name": [${name.value}, ${otherName1.value}, ${otherName2.value}],
                "rdfs:label": [${name.value}, ${otherName1.value}, ${otherName2.value}]
              }"""
            }
          )
        )
      }

      leftSchemaNames should contain theSameElementsAs List(name.value, otherName1.value, otherName2.value)

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      leftSchemaNames should contain oneOf (name.value, otherName1.value, otherName2.value)
    }
  }

  private trait TestCase {
    val triplesRemover = new IODuplicatePersonNameRemover(rdfStoreConfig, TestLogger())
    with DuplicatePersonNameRemover[IO]
  }

  private def leftSchemaNames =
    runQuery("SELECT ?name ?label WHERE { ?p schema:name ?name ; rdfs:label ?label . }")
      .unsafeRunSync()
      .flatMap(row => List(row("name"), row("label")))
      .toSet
}
