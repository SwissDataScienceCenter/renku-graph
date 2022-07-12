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

package io.renku.rdfstore

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.nonEmptyStrings
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.{Files, Paths}

class DatasetConfigFileSpec extends AnyWordSpec with should.Matchers {

  "fromConfigMap" should {

    "extract content of the config file defined in the given ConfigMap yaml" in {
      val yamlFile = Files.createTempFile("ds", "yaml")
      Files.writeString(yamlFile, configMap)

      object TestConfigFile extends DatasetConfigFileFactory[TestConfigFile](new TestConfigFile(_), ttlName, yamlFile)

      TestConfigFile.fromConfigMap() shouldBe TestConfigFile(configFileContent).asRight
    }

    "return a Left if cannot find the ds ttl body" in {
      val yamlFile = Files.createTempFile("ds", "yaml")
      Files.writeString(yamlFile, configMap)

      val otherTtlName = nonEmptyStrings().generateOne
      object TestConfigFile
          extends DatasetConfigFileFactory[TestConfigFile](new TestConfigFile(_), otherTtlName, yamlFile)

      val Left(failure) = TestConfigFile.fromConfigMap()

      failure.getMessage shouldBe s"No $otherTtlName found in $yamlFile or empty body"
    }

    "return a Left if yaml file cannot be found" in {
      val nonExistingYaml = Paths.get("non-existing.yaml")
      object TestConfigFile
          extends DatasetConfigFileFactory[TestConfigFile](new TestConfigFile(_), ttlName, nonExistingYaml)

      val Left(failure) = TestConfigFile.fromConfigMap()

      failure.printStackTrace()
      failure.getMessage shouldBe s"Problems while reading $nonExistingYaml"
    }
  }

  private lazy val ttlName = "ds.ttl"
  private lazy val configFileContent =
    """|
       |    @prefix:        <#> .
       |    @prefix fuseki: <http://jena.apache.org/fuseki#> .
       |    @prefix rdf:    <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
       |    @prefix rdfs:   <http://www.w3.org/2000/01/rdf-schema#> .
       |    @prefix tdb:    <http://jena.hpl.hp.com/2008/tdb#> .
       |
       |    :dsService rdf:type fuseki:Service ;
       |      rdfs:label                        "label" ;
       |      fuseki:name                       "name" ;
       |      fuseki:dataset                    :ds ;
       |    .
       |
       |    :ds rdf:type tdb:DatasetTDB ;
       |      tdb:location "/fuseki/databases/ds" ;
       |    .
       |""".stripMargin
  private lazy val configMap =
    s"""|apiVersion: v1
        |kind: ConfigMap
        |metadata:
        |  name: renku-ds-ttl
        |  labels:
        |    {{- include "jena.labels" . | nindent 4 }}
        |data:
        |  $ttlName: |-
        |    $configFileContent
        |""".stripMargin
}

private case class TestConfigFile private[rdfstore] (value: String) extends DatasetConfigFile
