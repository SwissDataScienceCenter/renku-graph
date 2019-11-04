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
import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.IORdfStoreClient.RdfDelete
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.triples._
import ch.datascience.rdfstore.triples.entities.Person
import ch.datascience.triplesgenerator.reprovisioning.IORdfStoreUpdater
import io.circe.literal._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class MailtoEmailRemoverSpec extends WordSpec with InMemoryRdfStore {

  "run" should {

    "do nothing if there's only valid email literals on Person entities" in new TestCase {

      val email = emails.generateOne

      loadToStore {
        triples(
          List(
            Person(Person.Id(names.generateOne), Some(email))
          )
        )
      }

      val initialStoreSize = rdfStoreSize

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      rdfStoreSize shouldBe initialStoreSize

      leftSchemaEmails shouldBe List(email.toString)
    }

    "remove Person triples where schema:email is a IRI starting with 'mailto:'" in new TestCase {

      val personId = Person.Id(names.generateOne)
      val email    = emails.generateOne

      loadToStore {
        triples(
          List(
            Person(personId, None) deepMerge {
              json"""{
                "schema:email": [${email.value}, { "@id": ${s"<mailto:$email>"} }]
              }"""
            }
          )
        )
      }

      leftSchemaEmails should contain theSameElementsAs List(email.toString, s"<mailto:$email>")

      triplesRemover.run.unsafeRunSync() shouldBe ((): Unit)

      leftSchemaEmails shouldBe List(email.toString)
    }
  }

  private trait TestCase {
    val triplesRemover = new IORdfStoreUpdater[RdfDelete](rdfStoreConfig, TestLogger()) with MailtoEmailRemover[IO]
  }

  private def leftSchemaEmails =
    runQuery("SELECT ?email WHERE { ?p schema:email ?email }")
      .unsafeRunSync()
      .map(row => row("email"))
}
