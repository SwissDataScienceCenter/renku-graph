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

package ch.datascience.triplesgenerator.reprovisioning

import ch.datascience.interpreters.TestLogger
import ch.datascience.rdfstore.InMemoryRdfStore
import ch.datascience.rdfstore.RdfStoreData.RDF
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class IOOrphanMailtoNoneRemoverSpec extends WordSpec with InMemoryRdfStore {

  "removeOrphanMailtoNoneTriples" should {

    "do nothing if the 'mailto:None' is used an object" in new TestCase {

      loadToStore {
        RDF(
          <rdf:Description rdf:about="file:///blob/4c0d6fc8b37c3b9a4dfeeee3c184fab018f9513b/data/some-dataset/README.rst">
            <rdf:type rdf:resource="http://schema.org/DigitalDocument"/>
            <schema:creator rdf:resource="mailto:None"/>
            <schema:isPartOf rdf:resource="https://renku.ch/testing/project"/>
            <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
            <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
            <schema:dateCreated>2019-07-31 09:19:57.694772</schema:dateCreated>
          </rdf:Description>
          <rdf:Description rdf:about="mailto:None">
            <rdf:type rdf:resource="http://schema.org/Person"/>
            <schema:email>user@mail.org</schema:email>
            <schema:name>User Name</schema:name>
          </rdf:Description>
        )
      }

      val initialStoreSize = rdfStoreSize

      triplesRemover.removeOrphanMailtoNoneTriples().unsafeRunSync() shouldBe ((): Unit)

      rdfStoreSize shouldBe initialStoreSize
    }

    "remove triples with the 'mailto:None' subject if such a resource is not used an object" in new TestCase {

      loadToStore {
        RDF(
          <rdf:Description rdf:about="file:///blob/4c0d6fc8b37c3b9a4dfeeee3c184fab018f9513b/data/some-dataset/README.rst">
            <rdf:type rdf:resource="http://schema.org/DigitalDocument"/>
            <schema:creator rdf:resource="mailto:user@mail.org"/>
            <schema:isPartOf rdf:resource="https://renku.ch/testing/project"/>
            <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
            <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
            <schema:dateCreated>2019-07-31 09:19:57.694772</schema:dateCreated>
          </rdf:Description>
          <rdf:Description rdf:about="mailto:None">
            <rdf:type rdf:resource="http://schema.org/Person"/>
            <schema:email>user@mail.org</schema:email>
            <schema:name>User Name</schema:name>
          </rdf:Description>
          <rdf:Description rdf:about="mailto:user@mail.org">
            <rdf:type rdf:resource="http://schema.org/Person"/>
            <schema:email>user@mail.org</schema:email>
            <schema:name>User Name</schema:name>
          </rdf:Description>
        )
      }

      mailToNoneTriples should be > 0

      val initialStoreSize = rdfStoreSize

      triplesRemover.removeOrphanMailtoNoneTriples().unsafeRunSync() shouldBe ((): Unit)

      rdfStoreSize      should not be initialStoreSize
      mailToNoneTriples shouldBe 0
    }
  }

  private trait TestCase {
    val triplesRemover = new IOOrphanMailtoNoneRemover(rdfStoreConfig, TestLogger())
  }

  private def mailToNoneTriples =
    runQuery("SELECT (COUNT(*) as ?triples) WHERE { <mailto:None> ?p ?o }")
      .unsafeRunSync()
      .map(row => row("triples"))
      .headOption
      .map(_.toInt)
      .getOrElse(throw new Exception("Cannot find the count of the 'mailto:None' triples"))
}
