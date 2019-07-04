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

package ch.datascience.triplesgenerator.generators

import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators._
import ch.datascience.triplesgenerator.config.DatasetType.{Mem, TDB}
import ch.datascience.triplesgenerator.config._
import ch.datascience.triplesgenerator.eventprocessing.RDFTriples
import org.scalacheck.Gen

object ServiceTypesGenerators {

  implicit val rdfTriplesSets: Gen[RDFTriples] = for {
    subject <- nonEmptyStrings()
    obj     <- nonEmptyStrings()
  } yield
    RDFTriples {
      <rdf:Description rdf:about={subject}>
        <rdfs:label>{obj}</rdfs:label>
      </rdf:Description>.toString()
    }

  implicit val fusekiAdminConfigs: Gen[FusekiAdminConfig] = for {
    fusekiUrl       <- httpUrls map FusekiBaseUrl.apply
    datasetName     <- nonEmptyStrings() map DatasetName.apply
    datasetType     <- Gen.oneOf(Mem, TDB)
    authCredentials <- basicAuthCredentials
  } yield FusekiAdminConfig(fusekiUrl, datasetName, datasetType, authCredentials)

  implicit val fusekiUserConfigs: Gen[FusekiUserConfig] = for {
    fusekiUrl       <- httpUrls map FusekiBaseUrl.apply
    datasetName     <- nonEmptyStrings() map DatasetName.apply
    authCredentials <- basicAuthCredentials
  } yield FusekiUserConfig(fusekiUrl, datasetName, authCredentials)

  implicit val schemaVersions: Gen[SchemaVersion] = Gen
    .listOfN(3, positiveInts(max = 50))
    .map(_.mkString("."))
    .map(SchemaVersion.apply)
}
