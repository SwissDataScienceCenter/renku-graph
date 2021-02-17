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

package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration
package persondetails

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.users.{Affiliation, Email, Name, ResourceId}
import ch.datascience.rdfstore.entities.bundles._
import ch.datascience.rdfstore.{FusekiBaseUrl, JsonLDTriples}
import ch.datascience.tinytypes.json.TinyTypeDecoders._
import ch.datascience.tinytypes.json.TinyTypeEncoders._
import io.circe.Json
import io.circe.literal.JsonStringContext
import io.circe.optics.JsonOptics._
import io.circe.optics.JsonPath.root
import monocle.function.Plated
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable
import scala.util.{Success, Try}
import cats.syntax.all._

class PersonExtractorSpec extends AnyWordSpec with should.Matchers with MockFactory with ScalaCheckPropertyChecks {

  "extractPersons" should {

    "return PersonData with multiple names " in new TestCase {
      forAll { personRawData: Set[PersonRawDatum] =>
        val jsonTriples = JsonLDTriples {
          val Some(value) = personRawData.map(_.toJson.asArray.map(_.toList)).toList.sequence
          value.flatten
        }

        val (_, actualPersonRawData) = personExtractor extractPersons jsonTriples
        actualPersonRawData shouldBe personRawData
      }
    }

    "return None when there is no resourceId" in new TestCase {

      val personWithoutIdJson = json"""[{
               "@type" : [
                 "http://www.w3.org/ns/prov#Person",
                 "http://schema.org/Person"
               ],
               "http://www.w3.org/2000/01/rdf-schema#label" : {
                 "@value" : "es mqlb"
               },
               "http://schema.org/affiliation" : {
                 "@value" : "tefotcvry"
               },
               "http://schema.org/email" : ${userEmails.generateNonEmptyList().toList},
               "http://schema.org/name" : ${userNames.generateNonEmptyList().toList}
             }]"""

      val jsonTriples = JsonLDTriples {
        val Some(value) = personWithoutIdJson.asArray.map(_.toList)
        value
      }

      val (_, actualPersonRawData) = personExtractor extractPersons jsonTriples
      actualPersonRawData shouldBe Set.empty[PersonRawDatum]

    }
  }

  private trait TestCase {
    val personExtractor = new PersonExtractorImpl()
  }

  type PersonRawDatum = (ResourceId, List[Name], List[Email])

  implicit class PersonRawDatumOps(data: PersonRawDatum) {
    def toJson: Json = {
      val names:  List[Json] = data._2.map(name => json"""{ "@value": ${name.value} }""")
      val emails: List[Json] = data._3.map(email => json"""{ "@value": ${email.value} }""")

      json"""[{
               "@id" : ${data._1.value},
               "@type" : [
                 "http://www.w3.org/ns/prov#Person",
                 "http://schema.org/Person"
               ],
               "http://www.w3.org/2000/01/rdf-schema#label" : {
                 "@value" : "es mqlb"
               },
               "http://schema.org/affiliation" : {
                 "@value" : "tefotcvry"
               },
               "http://schema.org/email" : $emails,
               "http://schema.org/name" : $names
             }]"""
    }
  }

}
