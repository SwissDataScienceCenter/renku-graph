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

package ch.datascience.graph.acceptancetests.stubs

import ch.datascience.graph.model.events.{CommitId, Project}
import com.github.tomakehurst.wiremock.client.WireMock.{get, ok, stubFor}

object RemoteTriplesGenerator {

  def `GET <triples-generator>/projects/:id/commits/:id returning OK with some triples`(project:  Project,
                                                                                        commitId: CommitId): Unit = {
    stubFor {
      get(s"/projects/${project.id}/commits/$commitId")
        .willReturn(
          ok(s"""
                |<rdf:RDF
                |   xmlns:ns1="http://purl.org/dc/terms/"
                |   xmlns:ns2="http://www.w3.org/ns/prov#"
                |   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                |   xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
                |  <rdf:Description rdf:about="file:///commit/$commitId">
                |    <ns2:startedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2019-05-01T14:35:48+02:00</ns2:startedAtTime>
                |    <ns1:isPartOf rdf:resource="https://dev.renku.ch/${project.path}"/>
                |    <rdfs:comment>Added line</rdfs:comment>
                |    <rdfs:label>$commitId</rdfs:label>
                |    <ns2:endedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2019-05-01T14:35:48+02:00</ns2:endedAtTime>
                |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
                |    <ns2:wasInformedBy rdf:resource="file:///commit/b318d7f13ae126aa27f3c6600e505cf116e7ba27"/>
                |  </rdf:Description>
                |  <rdf:Description rdf:about="file:///blob/$commitId/new-file.txt">
                |    <ns2:atLocation>new-file.txt</ns2:atLocation>
                |    <ns2:qualifiedGeneration rdf:resource="file:///commit/$commitId/tree/new-file.txt"/>
                |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
                |    <ns1:isPartOf rdf:resource="https://dev.renku.ch/${project.path}"/>
                |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
                |    <rdfs:label>new-file.txt@$commitId</rdfs:label>
                |  </rdf:Description>
                |  <rdf:Description rdf:about="file:///commit/$commitId/tree/new-file.txt">
                |    <ns2:activity rdf:resource="file:///commit/$commitId"/>
                |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
                |  </rdf:Description>
                |  <rdf:Description rdf:about="https://dev.renku.ch/${project.path}">
                |    <rdf:type rdf:resource="http://xmlns.com/foaf/0.1/Project"/>
                |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Location"/>
                |  </rdf:Description>
                |</rdf:RDF>
          """.stripMargin)
        )
    }
    ()
  }
}
