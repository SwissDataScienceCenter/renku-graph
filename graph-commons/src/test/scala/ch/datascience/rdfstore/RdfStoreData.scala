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

import ch.datascience.config.RenkuBaseUrl
import ch.datascience.generators.CommonGraphGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.SchemaVersion
import ch.datascience.graph.model.dataSets._
import ch.datascience.graph.model.events.EventsGenerators._
import ch.datascience.graph.model.events.{CommitId, ProjectPath}
import ch.datascience.graph.model.users.{Email, Name => UserName}
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import org.scalacheck.Gen

import scala.xml.{Elem, NodeBuffer, NodeSeq}

object RdfStoreData extends RdfStoreData(RenkuBaseUrl("https://dev.renku.ch"))

class RdfStoreData(val renkuBaseUrl: RenkuBaseUrl) {

  def RDF(triples: NodeBuffer*): Elem =
    <rdf:RDF
    xmlns:prov="http://www.w3.org/ns/prov#"
    xmlns:schema="http://schema.org/"
    xmlns:dcterms="http://purl.org/dc/terms/"
    xmlns:foaf="http://xmlns.com/foaf/0.1/"
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
          <prov:agent rdf:resource={agentNodeResource(schemaVersion)}/>
        }.getOrElse(NodeSeq.Empty)}
        <rdfs:label>{commitId}</rdfs:label>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <prov:wasInformedBy rdf:resource={s"file:///commit/$commitId"}/>
      </rdf:Description> &+
      {maybeSchemaVersion.map(agentNode).getOrElse(NodeSeq.Empty)} &+
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

  def singleFileAndCommitWithDataset(
      projectPath:               ProjectPath,
      commitId:                  CommitId = commitIds.generateOne,
      committerEmail:            Email = emails.generateOne,
      committerName:             UserName = names.generateOne,
      dataSetId:                 Identifier = dataSetIds.generateOne,
      dataSetName:               Name = dataSetNames.generateOne,
      maybeDataSetDescription:   Option[Description] = Gen.option(dataSetDescriptions).generateOne,
      dataSetCreatedDate:        CreatedDate = dataSetCreatedDates.generateOne,
      maybeDataSetPublishedDate: Option[PublishedDate] = Gen.option(dataSetPublishedDates).generateOne,
      maybeDataSetCreators:      Set[(Option[Email], UserName)] = setOf(dataSetCreators).generateOne,
      schemaVersion:             SchemaVersion = schemaVersions.generateOne): NodeBuffer =
    // format: off
    <rdf:Description rdf:about={s"file:///commit/$commitId/tree/.gitattributes"}>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
      <prov:activity rdf:resource={s"file:///commit/$commitId"}/>
    </rdf:Description>
    <rdf:Description rdf:about={s"file:///commit/$commitId"}>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
      <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      <prov:influenced rdf:resource={s"file:///blob/$commitId/.renku"}/>
      <prov:influenced rdf:resource={s"file:///blob/$commitId/.renku/datasets"}/>
      <prov:influenced rdf:resource={s"file:///blob/$commitId/.renku/datasets/$dataSetId"}/>
      <prov:agent rdf:resource={agentNodeResource(schemaVersion)}/>
      <prov:agent rdf:resource={personNodeResource(committerName)}/>
      <prov:wasInformedBy rdf:resource="file:///commit/4c0d6fc8b37c3b9a4dfeeee3c184fab018f9513b"/>
      <rdfs:label>{commitId.toString}</rdfs:label>
      <rdfs:comment>{s"renku dataset add $dataSetName https://raw.githubusercontent.com/SwissDataScienceCenter/renku-python/master/README.rst"}</rdfs:comment>
      <prov:startedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2019-07-31T09:19:57+00:00</prov:startedAtTime>
      <prov:endedAtTime rdf:datatype="http://www.w3.org/2001/XMLSchema#dateTime">2019-07-31T09:19:57+00:00</prov:endedAtTime>
    </rdf:Description> &+
    personNode(committerName, Some(committerEmail)) &+
    <rdf:Description rdf:about={s"file:///blob/4c0d6fc8b37c3b9a4dfeeee3c184fab018f9513b/data/$dataSetName/README.rst"}>
      <rdf:type rdf:resource="http://schema.org/DigitalDocument"/>
      <prov:atLocation>{s"data/$dataSetName/README.rst"}</prov:atLocation>
      <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      <rdfs:label>{s"data/$dataSetName/README.rst@4c0d6fc8b37c3b9a4dfeeee3c184fab018f9513b"}</rdfs:label>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      <schema:url>https://raw.githubusercontent.com/SwissDataScienceCenter/renku-python/master/README.rst</schema:url>
      <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      <schema:dateCreated>2019-07-31 09:19:57.694772</schema:dateCreated>
    </rdf:Description> &+
    agentNode(schemaVersion) &+
    <rdf:Description rdf:about={s"file:///blob/$commitId/.renku"}>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      <prov:atLocation>.renku</prov:atLocation>
      <prov:hadMember rdf:resource={s"file:///blob/$commitId/.renku/datasets"}/>
      <rdfs:label>{s".renku@$commitId"}</rdfs:label>
    </rdf:Description>
    <rdf:Description rdf:about={s"file:///blob/$commitId/.renku/datasets/$dataSetId"}>
      <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
      <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      <prov:atLocation>{s".renku/datasets/$dataSetId"}</prov:atLocation>
      <prov:hadMember rdf:resource={s"file:///$dataSetId"}/>
      <rdfs:label>{s".renku/datasets/$dataSetId@$commitId"}</rdfs:label>
    </rdf:Description>
    <rdf:Description rdf:about="https://dev.renku.ch/jakub.chrobasik/kuba-bikes">
      <rdf:type rdf:resource="http://xmlns.com/foaf/0.1/Project"/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Location"/>
    </rdf:Description>
    <rdf:Description rdf:about={s"file:///commit/$commitId/tree/home/jovyan/kuba-bikes/.renku/datasets/$dataSetId"}>
      <prov:activity rdf:resource={s"file:///commit/$commitId"}/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
    </rdf:Description>
    <rdf:Description rdf:about={s"file:///blob/$commitId/.renku.lock"}>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      <prov:atLocation>.renku.lock</prov:atLocation>
      <prov:qualifiedGeneration rdf:resource={s"file:///commit/$commitId/tree/.renku.lock"}/>
      <rdfs:label>{s".renku.lock@$commitId"}</rdfs:label>
    </rdf:Description>
    <rdf:Description rdf:about={s"file:///$dataSetId"}>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      <rdf:type rdf:resource="http://schema.org/Dataset"/>
      <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      <prov:atLocation>{s"/home/jovyan/kuba-bikes/.renku/datasets/$dataSetId"}</prov:atLocation>
      <schema:hasPart rdf:resource={s"file:///blob/4c0d6fc8b37c3b9a4dfeeee3c184fab018f9513b/data/$dataSetName/README.rst"}/>
      <prov:qualifiedGeneration rdf:resource={s"file:///commit/$commitId/tree/home/jovyan/kuba-bikes/.renku/datasets/$dataSetId"}/>
      <schema:identifier>{dataSetId.toString}</schema:identifier>
      <rdfs:label>{dataSetId.toString}</rdfs:label>
      <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      <schema:name>{dataSetName.toString}</schema:name>
      <schema:dateCreated>{dataSetCreatedDate.toString}</schema:dateCreated>
      {maybeDataSetCreators.map { creator =>
        <schema:creator rdf:resource={personNodeResource(creator._2)}/>
      }}
      {maybeDataSetPublishedDate.map { publishedDate =>
        <schema:datePublished rdf:datatype="http://schema.org/Date">{publishedDate.toString}</schema:datePublished>
      }.getOrElse(NodeSeq.Empty)}
      {maybeDataSetDescription.map { description =>
        <schema:description>{description.toString}</schema:description>
      }.getOrElse(NodeSeq.Empty)}
    </rdf:Description> ++
    {maybeDataSetCreators.map { creator =>
      personNode(creator._2, creator._1)
    }} ++
    <rdf:Description rdf:about={s"file:///blob/$commitId/.gitattributes"}>
      <rdfs:label>{s".gitattributes@$commitId"}</rdfs:label>
      <prov:qualifiedGeneration rdf:resource={s"file:///commit/$commitId/tree/.gitattributes"}/>
      <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      <prov:atLocation>.gitattributes</prov:atLocation>
      <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
    </rdf:Description>
    <rdf:Description rdf:about={s"file:///blob/$commitId/.renku/datasets"}>
      <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      <rdfs:label>{s".renku/datasets@$commitId"}</rdfs:label>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      <prov:atLocation>.renku/datasets</prov:atLocation>
      <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      <prov:hadMember rdf:resource={s"file:///blob/$commitId/.renku/datasets/$dataSetId"}/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
    </rdf:Description>
    <rdf:Description rdf:about={s"file:///commit/$commitId/tree/.renku.lock"}>
      <prov:activity rdf:resource={s"file:///commit/$commitId"}/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
    </rdf:Description>
    // format: on

  private val dataSetCreators: Gen[(Option[Email], UserName)] = for {
    maybeEmail <- Gen.option(emails)
    name       <- names
  } yield (maybeEmail, name)

  def multiFileAndCommit(
      projectPath:   ProjectPath,
      schemaVersion: SchemaVersion = schemaVersions.generateOne
  ) = new MultiFileAndCommitTriples(projectPath, schemaVersion)

  object MultiFileAndCommitTriples {

    final class ResourceName private (val value: String) extends StringTinyType
    object ResourceName extends TinyTypeFactory[ResourceName](new ResourceName(_)) with NonBlank
    final class ResourceLabel private (val value: String) extends StringTinyType
    object ResourceLabel extends TinyTypeFactory[ResourceLabel](new ResourceLabel(_)) with NonBlank
    case class Resource(name: ResourceName, label: ResourceLabel)
  }

  class MultiFileAndCommitTriples(projectPath: ProjectPath, schemaVersion: SchemaVersion) {

    import MultiFileAndCommitTriples._

    // format: off
    val commit4Id: CommitId                   = CommitId("0000004")
    val resultFile1: String                   = "result-file-1"
    val `commit1-input-data`: Resource        = resource(name = s"file:///blob/0000001/input-data",        label = s"input-data@0000001")
    val `commit2-source-file1`: Resource      = resource(name = s"file:///blob/0000002/source-file-1",     label = s"source-file-1@0000002")
    val `commit2-source-file2`: Resource      = resource(name = s"file:///blob/0000002/source-file-2",     label = s"source-file-2@0000002")
    val `commit3-renku-run`: Resource         = resource(name = s"file:///commit/0000003",                 label = s"renku run python source-file-1 input-data preprocessed-data")
    val `commit3-preprocessed-data`: Resource = resource(name = s"file:///blob/0000003/preprocessed-data", label = s"preprocessed-data@0000003")
    val `commit4-renku-run`: Resource         = resource(name = s"file:///commit/$commit4Id",              label = s"renku run python source-file-2 preprocessed-data")
    val `commit4-result-file1`: Resource      = resource(name = s"file:///blob/$commit4Id/$resultFile1",   label = s"$resultFile1@$commit4Id")
    val `commit4-result-file2`: Resource      = resource(name = s"file:///blob/$commit4Id/result-file-2",  label = s"result-file-2@$commit4Id")
    // format: on

    private def resource(name: String, label: String) = Resource(ResourceName(name), ResourceLabel(label))

    val triples: NodeBuffer =
      // format: off
      <rdf:Description rdf:about="file:///commit/0000001">
        <rdfs:label>0000001</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:comment>renku dataset add zhbikes https://data.stadt-zuerich.ch/dataset/verkehrszaehlungen_werte_fussgaenger_velo/resource/d17a0a74-1073-46f0-a26e-46a403c061ec/download/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv</rdfs:comment>
        <prov:agent rdf:resource={agentNodeResource(schemaVersion)}/>
      </rdf:Description>
      <rdf:Description rdf:about={s"${`commit4-renku-run`.name}/outputs/output_1"}>
        <prov:activity rdf:resource={`commit4-renku-run`.name.toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
      </rdf:Description>
      <rdf:Description rdf:about={`commit4-renku-run`.name.toString}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
        <prov:qualifiedAssociation rdf:resource={s"${`commit4-renku-run`.name}/association"}/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#ProcessRun"/>
        <rdfs:comment>{`commit4-renku-run`.label.toString}</rdfs:comment>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>{s".renku/workflow/5e5ac7d7efcc4d829da8b19f9b900a11_python.cwl@$commit4Id"}</rdfs:label>
        <prov:qualifiedUsage rdf:resource={s"${`commit4-renku-run`.name}/inputs/input_2"}/>
        <prov:qualifiedUsage rdf:resource={s"${`commit4-renku-run`.name}/inputs/input_1"}/>
        <prov:agent rdf:resource={agentNodeResource(schemaVersion)}/>
      </rdf:Description>
      <rdf:Description rdf:about={s"file:///blob/$commit4Id/.renku/workflow/5e5ac7d7efcc4d829da8b19f9b900a11_python.cwl"}>
        <rdfs:label>{s".renku/workflow/5e5ac7d7efcc4d829da8b19f9b900a11_python.cwl@$commit4Id"}</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Plan"/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfdesc#Process"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///commit/0000001/tree/.gitattributes">
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
        <prov:activity rdf:resource="file:///commit/0000001"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"${`commit4-renku-run`.name}/inputs/input_2"}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Usage"/>
        <prov:entity rdf:resource={s"${`commit3-preprocessed-data`.name.toString}"}/>
      </rdf:Description>
      <rdf:Description rdf:about={s"${`commit3-renku-run`.name}/association"}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Association"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"${`commit3-renku-run`.name}/inputs/input_2"}>
        <prov:entity rdf:resource={`commit1-input-data`.name.toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Usage"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000002/src">
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <schema:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>src@0000002</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"${`commit4-renku-run`.name}/association"}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Association"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///commit/0000002">
        <rdfs:comment>added refactored scripts</rdfs:comment>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>0000002</rdfs:label>
        <prov:agent rdf:resource={agentNodeResource(schemaVersion)}/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///commit/0000001/tree/input-data/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv">
        <prov:activity rdf:resource="file:///commit/0000001"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
      </rdf:Description>
      <rdf:Description rdf:about={`commit3-renku-run`.name.toString}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
        <rdfs:label>.renku/workflow/84ff4b53ae634b589f2e83e7c36bd45a_python.cwl@0000003</rdfs:label>
        <prov:qualifiedAssociation rdf:resource={s"${`commit3-renku-run`.name}/association"}/>
        <prov:qualifiedUsage rdf:resource={s"${`commit3-renku-run`.name}/inputs/input_1"}/>
        <prov:qualifiedUsage rdf:resource={s"${`commit3-renku-run`.name}/inputs/input_2"}/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#ProcessRun"/>
        <rdfs:comment>{`commit3-renku-run`.label.toString}</rdfs:comment>
        <prov:agent rdf:resource={agentNodeResource(schemaVersion)}/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///commit/0000001/tree/.renku/refs/datasets/zhbikes">
        <prov:activity rdf:resource="file:///commit/0000001"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml">
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <prov:qualifiedGeneration rdf:resource="file:///commit/0000001/tree/.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml"/>
        <rdfs:label>.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml@0000001</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/.renku/datasets/f0d5e338c7644f1995484ac00108d525">
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <rdfs:label>.renku/datasets/f0d5e338c7644f1995484ac00108d525@0000001</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/data">
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <rdfs:label>data@0000001</rdfs:label>
      </rdf:Description>
      <rdf:Description rdf:about="file:///commit/0000002/tree/source-file-2">
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
        <prov:activity rdf:resource="file:///commit/0000002"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/.renku/refs/datasets">
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>.renku/refs/datasets@0000001</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      </rdf:Description>
      <rdf:Description rdf:about={`commit2-source-file1`.name.toString}>
        <rdfs:label>{`commit2-source-file1`.label.toString}</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <prov:qualifiedGeneration rdf:resource="file:///commit/0000002/tree/source-file-1"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/.renku/refs">
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>.renku/refs@0000001</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      </rdf:Description>
      <rdf:Description rdf:about={`commit3-preprocessed-data`.name.toString}>
        <prov:qualifiedGeneration rdf:resource={s"${`commit3-renku-run`.name}/outputs/output_0"}/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>{`commit3-preprocessed-data`.label.toString}</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/.renku">
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>.renku@0000001</rdfs:label>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"${`commit3-renku-run`.name}/outputs/output_0"}>
        <prov:activity rdf:resource={`commit3-renku-run`.name.toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
      </rdf:Description>
      <rdf:Description rdf:about={`commit4-result-file1`.name.toString}>
        <rdfs:label>{`commit4-result-file1`.label.toString}</rdfs:label>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <prov:qualifiedGeneration rdf:resource={s"${`commit4-renku-run`.name}/outputs/output_1"}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/input-data/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv">
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <prov:qualifiedGeneration rdf:resource="file:///commit/0000001/tree/input-data/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdfs:label>input-data/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv@0000001</rdfs:label>
      </rdf:Description>
      <rdf:Description rdf:about={`commit1-input-data`.name.toString}>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <rdfs:label>{`commit1-input-data`.label.toString}</rdfs:label>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"${`commit3-renku-run`.name}/inputs/input_1"}>
        <prov:entity rdf:resource={`commit2-source-file1`.name.toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Usage"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/.gitattributes">
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <prov:qualifiedGeneration rdf:resource="file:///commit/0000001/tree/.gitattributes"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <rdfs:label>.gitattributes@0000001</rdfs:label>
      </rdf:Description>
      <rdf:Description rdf:about={`commit2-source-file2`.name.toString}>
        <rdfs:label>source-file-2@0000002</rdfs:label>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <prov:qualifiedGeneration rdf:resource="file:///commit/0000002/tree/source-file-2"/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      </rdf:Description>
      <rdf:Description rdf:about={`commit4-result-file2`.name.toString}>
        <rdfs:label>{`commit4-result-file2`.label.toString}</rdfs:label>
        <prov:qualifiedGeneration rdf:resource={s"${`commit4-renku-run`.name}/outputs/output_0"}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/.renku/refs/datasets/zhbikes">
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>.renku/refs/datasets/zhbikes@0000001</rdfs:label>
        <prov:qualifiedGeneration rdf:resource="file:///commit/0000001/tree/.renku/refs/datasets/zhbikes"/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000001/.renku/datasets">
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdfs:label>.renku/datasets@0000001</rdfs:label>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///blob/0000003/.renku/workflow/84ff4b53ae634b589f2e83e7c36bd45a_python.cwl">
        <rdf:type rdf:resource="http://purl.org/wf4ever/wfdesc#Process"/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
        <dcterms:isPartOf rdf:resource={(renkuBaseUrl / projectPath).toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Plan"/>
        <rdfs:label>.renku/workflow/84ff4b53ae634b589f2e83e7c36bd45a_python.cwl@0000003</rdfs:label>
      </rdf:Description>
      <rdf:Description rdf:about={s"${`commit4-renku-run`.name}/outputs/output_0"}>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
        <prov:activity rdf:resource={`commit4-renku-run`.name.toString}/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///commit/0000002/tree/source-file-1">
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
        <prov:activity rdf:resource="file:///commit/0000002"/>
      </rdf:Description>
      <rdf:Description rdf:about="file:///commit/0000001/tree/.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml">
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
        <prov:activity rdf:resource="file:///commit/0000001"/>
      </rdf:Description>
      <rdf:Description rdf:about={s"${`commit4-renku-run`.name}/inputs/input_1"}>
        <prov:entity rdf:resource={`commit2-source-file2`.name.toString}/>
        <rdf:type rdf:resource="http://www.w3.org/ns/prov#Usage"/>
      </rdf:Description> &+
      agentNode(schemaVersion)
      // format: on
  }

  private def agentNode(schemaVersion: SchemaVersion) =
    <rdf:Description rdf:about={agentNodeResource(schemaVersion)}>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#SoftwareAgent"/>
      <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#WorkflowEngine"/>
      <rdfs:label>{s"renku $schemaVersion"}</rdfs:label>
    </rdf:Description>

  private def agentNodeResource(schemaVersion: SchemaVersion) =
    s"https://github.com/swissdatasciencecenter/renku-python/tree/v$schemaVersion"

  private def personNode(name: UserName, maybeEmail: Option[Email]) =
    <rdf:Description rdf:about={personNodeResource(name)}>
      <rdf:type rdf:resource="http://schema.org/Person"/>
      <rdf:type rdf:resource="http://www.w3.org/ns/prov#Person"/>
      <schema:name>{name.toString}</schema:name>
      {maybeEmail.map { email =>
      <schema:email>{email.toString}</schema:email>
      }.getOrElse(NodeSeq.Empty)}
    </rdf:Description>

  private def personNodeResource(name: UserName) = s"file:///_${name.value.replace(" ", "-")}"
}
