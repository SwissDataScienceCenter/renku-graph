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

package io.renku.triplesstore

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Paths}

class DatasetConfigFileSpec extends AnyWordSpec with should.Matchers {

  "fromConfigMap" should {

    "read content of the TTL file" in {
      val ttlFile = Files.createTempFile("ds", "ttl")
      Files.writeString(ttlFile, ttlFileContent)

      object TestConfigFile
          extends DatasetConfigFileFactory[TestConfigFile](nonEmptyStrings().generateAs(DatasetName),
                                                           new TestConfigFile(_),
                                                           ttlFile.toString
          )

      TestConfigFile.fromTtlFile() shouldBe TestConfigFile(ttlFileContent).asRight
    }

    "return a Left for an empty file" in {
      val ttlFile = Files.createTempFile("ds", "ttl")
      Files.writeString(ttlFile, "")

      object TestConfigFile
          extends DatasetConfigFileFactory[TestConfigFile](nonEmptyStrings().generateAs(DatasetName),
                                                           new TestConfigFile(_),
                                                           ttlFile.toString
          )

      val Left(failure) = TestConfigFile.fromTtlFile()

      failure.getMessage shouldBe s"$ttlFile is empty"
    }

    "return a Left if TTL file cannot be found" in {
      val nonExistingTtl = Paths.get("non-existing.yaml")
      object TestConfigFile
          extends DatasetConfigFileFactory[TestConfigFile](nonEmptyStrings().generateAs(DatasetName),
                                                           new TestConfigFile(_),
                                                           nonExistingTtl.toString
          )

      val Left(failure) = TestConfigFile.fromTtlFile()

      failure.getMessage shouldBe s"Problems while reading $nonExistingTtl"
    }
  }

  private lazy val ttlFileContent =
    """|@prefix:        <#> .
       |@prefix fuseki: <http://jena.apache.org/fuseki#> .
       |@prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
       |@prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .
       |@prefix tdb:    <http://jena.hpl.hp.com/2008/tdb#> .
       |
       |:dsService rdf:type fuseki:Service ;
       |  rdfs:label                        "label" ;
       |  fuseki:name                       "name" ;
       |  fuseki:dataset                    :ds ;
       |.
       |
       |:ds rdf:type tdb:DatasetTDB ;
       |  tdb:location "/fuseki/databases/ds" ;
       |.""".stripMargin
}

private case class TestConfigFile private[triplesstore] (value: String) extends DatasetConfigFile
