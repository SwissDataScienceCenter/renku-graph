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

package ch.datascience.triplesgenerator.rdfstore

import ch.datascience.config.RenkuBaseUrl
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.triplesgenerator.config.SchemaVersion

import scala.xml.Null

object TestData extends TestData(RenkuBaseUrl("https://dev.renku.ch"))

class TestData(val renkuBaseUrl: RenkuBaseUrl) {

  def minimalTriples(projectPath: ProjectPath, commitId: CommitId, maybeSchemaVersion: Option[SchemaVersion]): String =
    <rdf:RDF 
    xmlns:prov="http://www.w3.org/ns/prov#" 
    xmlns:dcterms="http://purl.org/dc/terms/" 
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" 
    xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
      <rdf:Description rdf:about={s"""file:///commit/$commitId"""}>
        <rdfs:comment>some change</rdfs:comment>
        <rdfs:label>{commitId}</rdfs:label>
        <prov:endedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2018-12-06T11:26:33+01:00</prov:endedAtTime>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <prov:startedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2018-12-06T11:26:33+01:00</prov:startedAtTime>
        <prov:wasInformedBy rdf:resource={s"{file:///commit/$commitId}"}/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commitId/README.md"}>
        <prov:qualifiedGeneration rdf:resource={s"file:///commit/$commitId/tree/README.md"}/>
        <prov:atLocation>README.md</prov:atLocation>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>{s"README.md@$commitId"}</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///commit/$commitId/tree/README.md"}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
        <prov:activity rdf:resource={s"file:///commit/$commitId"}/>
      </rdf:Description>
      <rdf:Description rdf:about={(renkuBaseUrl / projectPath).toString}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Location"/>
        <rdf:type rdf:resource="http://xmlns.com/foaf/0.1/Project"/>
        {maybeSchemaVersion.map{ version => <dcterms:hasVersion>{version.toString}</dcterms:hasVersion>}.getOrElse(Null)}
      </rdf:Description>
    </rdf:RDF>.toString()
}
