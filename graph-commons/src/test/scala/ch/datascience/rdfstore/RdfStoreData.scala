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

package ch.datascience.rdfstore

import java.util.UUID

import ch.datascience.config.RenkuBaseUrl
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.tinytypes.Renderer

import scala.xml.{Elem, NodeBuffer, NodeSeq}

object RdfStoreData extends RdfStoreData(RenkuBaseUrl("https://dev.renku.ch"))

class RdfStoreData(val renkuBaseUrl: RenkuBaseUrl) {

  def RDF(triples: NodeBuffer*): Elem =
    <rdf:RDF
    xmlns:prov="http://www.w3.org/ns/prov#"
    xmlns:schema="http://schema.org/"
    xmlns:dcterms="http://purl.org/dc/terms/"
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
      {triples.reduce(_ &+ _)}
    </rdf:RDF>

  def singleFileAndCommitTriples(projectPath:        ProjectPath,
                                 commitId:           CommitId,
                                 maybeSchemaVersion: Option[SchemaVersion]): NodeBuffer =
    // format: off
      <rdf:Description rdf:about={(renkuBaseUrl / projectPath).toString}>
        <rdf:type rdf:resource="http://xmlns.com/foaf/0.1/Project"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Location"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///commit/$commitId"}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
        <prov:startedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2018-12-06T11:26:33+01:00</prov:startedAtTime>
        <prov:endedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2018-12-06T11:26:33+01:00</prov:endedAtTime>
        <rdfs:comment>some change</rdfs:comment>
        {maybeSchemaVersion.map { schemaVersion =>
          <prov:agent rdf:resource={s"https://github.com/swissdatasciencecenter/renku-python/tree/${schemaVersion.showAs[ShaLike]}"}/>
        }.getOrElse(NodeSeq.Empty)}
        <rdfs:label>{commitId}</rdfs:label>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <prov:wasInformedBy rdf:resource={s"file:///commit/$commitId"}/>
      </rdf:Description> &+
      {maybeSchemaVersion.map { schemaVersion =>
          <rdf:Description rdf:about={s"https://github.com/swissdatasciencecenter/renku-python/tree/${schemaVersion.showAs[ShaLike]}"}>
            <rdf:type rdf:resource="http://www.w3.org/ns/prov#SoftwareAgent"/>
            <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#WorkflowEngine"/>
            <rdfs:label>{s"renku $schemaVersion"}</rdfs:label>
          </rdf:Description>
      }.getOrElse(NodeSeq.Empty)} &+
      <rdf:Description rdf:about={s"file:///blob/$commitId/README.md"}>
        <prov:qualifiedGeneration rdf:resource={s"file:///commit/$commitId/tree/README.md"}/>
        <prov:atLocation>README.md</prov:atLocation>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdfs:label>{s"README.md@$commitId"}</rdfs:label>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///commit/$commitId/tree/README.md"}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
        <prov:activity rdf:resource={s"file:///commit/$commitId"}/>
      </rdf:Description>
  // format: on

  def singleFileAndCommitWithDataset(projectPath:   ProjectPath,
                                     commitId:      CommitId,
                                     schemaVersion: SchemaVersion,
                                     datasetId:     String = UUID.randomUUID().toString): NodeBuffer =
    // format: off
    <rdf:Description rdf:about={s"file:///commit/$commitId/tree/.renku/datasets/$datasetId/metadata.yml"}>
      <prov:activity rdf:resource={s"file:///commit/$commitId"}/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
    </rdf:Description>
      <rdf:Description rdf:about={s"file:///commit/$commitId"}>
        <prov:influenced rdf:resource={s"file:///blob/$commitId/data"}/>
        <prov:influenced rdf:resource={s"file:///blob/$commitId/data/datasetname"}/>
        <prov:influenced rdf:resource={s"file:///blob/$commitId/.renku"}/>
        <prov:endedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2019-07-15T10:13:39+02:00</prov:endedAtTime>
        <rdfs:label>{s"$commitId"}</rdfs:label>
        <prov:influenced rdf:resource={s"file:///blob/$commitId/.renku/datasets/$datasetId"}/>
        <prov:agent rdf:resource={s"https://github.com/swissdatasciencecenter/renku-python/tree/${schemaVersion.showAs[ShaLike]}"}/>
        <rdfs:comment>renku dataset add zhbikes https://data.stadt-zuerich.ch/dataset/verkehrszaehlungen_werte_fussgaenger_velo/resource/d17a0a74-1073-46f0-a26e-46a403c061ec/download/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv</rdfs:comment>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <prov:influenced rdf:resource={s"file:///blob/$commitId/.renku/datasets"}/>
        <prov:startedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2019-07-15T12:13:37+02:00</prov:startedAtTime>
        <prov:wasInformedBy rdf:resource={s"file:///commit/$commitId"}/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commitId/.renku"}>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <prov:hadMember rdf:resource={s"file:///blob/$commitId/.renku/datasets"}/>
        <prov:atLocation>.renku</prov:atLocation>
        <rdfs:label>{s".renku@$commitId"}</rdfs:label>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commitId/data"}>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <prov:atLocation>data</prov:atLocation>
        <prov:hadMember rdf:resource={s"file:///blob/$commitId/data/datasetname"}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdfs:label>{s"data@$commitId"}</rdfs:label>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commitId/data/datasetname"}>
        <rdfs:label>{s"data/zhbikes@$commitId"}</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <prov:hadMember rdf:resource={s"file:///blob/$commitId/data/zhbikes/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv"}/>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <prov:atLocation>data/zhbikes</prov:atLocation>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commitId/.renku/datasets/$datasetId"}>
        <rdfs:label>{s".renku/datasets/$datasetId@$commitId"}</rdfs:label>
        <prov:hadMember rdf:resource={s"file:///blob/$commitId/.renku/datasets/$datasetId/metadata.yml"}/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <prov:atLocation>{s".renku/datasets/$datasetId"}</prov:atLocation>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commitId/.renku/datasets/$datasetId/metadata.yml"}>
        <prov:atLocation>{s".renku/datasets/$datasetId/metadata.yml"}</prov:atLocation>
        <prov:qualifiedGeneration rdf:resource={s"file:///commit/$commitId/tree/.renku/datasets/$datasetId/metadata.yml"}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>{s".renku/datasets/$datasetId/metadata.yml@$commitId"}</rdfs:label>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      </rdf:Description>
      <rdf:Description rdf:about={(renkuBaseUrl / projectPath).toString}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Location"/>
        <rdf:type rdf:resource="http://xmlns.com/foaf/0.1/Project"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commitId/.renku.lock"}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <rdfs:label>{s".renku.lock@$commitId"}</rdfs:label>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <prov:atLocation>.renku.lock</prov:atLocation>
        <prov:qualifiedGeneration rdf:resource={s"file:///commit/$commitId/tree/.renku.lock"}/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///commit/$commitId/tree/data/zhbikes/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv"}>
        <prov:activity rdf:resource={s"file:///commit/$commitId"}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commitId/data/zhbikes/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv"}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <prov:qualifiedGeneration rdf:resource={s"file:///commit/$commitId/tree/data/zhbikes/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv"}/>
        <prov:atLocation>data/zhbikes/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv</prov:atLocation>
        <rdfs:label>{s"data/zhbikes/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv@$commitId"}</rdfs:label>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commitId/.renku/datasets"}>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <prov:atLocation>.renku/datasets</prov:atLocation>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <prov:hadMember rdf:resource={s"file:///blob/$commitId/.renku/datasets/$datasetId"}/>
        <rdfs:label>{s".renku/datasets@$commitId"}</rdfs:label>
      </rdf:Description>
      <rdf:Description rdf:about={s"https://github.com/swissdatasciencecenter/renku-python/tree/${schemaVersion.showAs[ShaLike]}"}>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#WorkflowEngine"/>
        <rdfs:label>{s"renku $schemaVersion"}</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#SoftwareAgent"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///commit/$commitId/tree/.renku.lock"}>
        <prov:activity rdf:resource={s"file:///commit/$commitId"}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
      </rdf:Description>
  // format: on

  private trait ShaLike
  private object ShaLike {
    implicit val shaLikeRenderer: Renderer[ShaLike, SchemaVersion] = _.value.replace(".", "")
  }
}
