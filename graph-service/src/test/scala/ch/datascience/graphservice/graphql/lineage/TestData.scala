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

package ch.datascience.graphservice.graphql.lineage

import ch.datascience.graph.model.events.ProjectPath
import ch.datascience.graphservice.config.RenkuBaseUrl
import ch.datascience.graphservice.graphql.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.graphservice.graphql.lineage.model.{Node, NodeId, NodeLabel}

object TestData {

  // format: off
  val `commit1-input-data`: Node   =      node(id = "file:///blob/0000001/input-data",        label = "input-data@0000001")
  val `commit2-source-file1`: Node =      node(id = "file:///blob/0000002/source-file-1",     label = "source-file-1@0000002")
  val `commit2-source-file2`: Node =      node(id = "file:///blob/0000002/source-file-2",     label = "source-file-2@0000002")
  val `commit3-renku-run`: Node =         node(id = "file:///commit/0000003",                 label = "renku run python source-file-1 input-data preprocessed-data")
  val `commit3-preprocessed-data`: Node = node(id = "file:///blob/0000003/preprocessed-data", label = "preprocessed-data@0000003")
  val `commit4-renku-run`: Node =         node(id = "file:///commit/0000004",                 label = "renku run python source-file-2 preprocessed-data")
  val `commit4-result-file1`: Node =      node(id = "file:///blob/0000004/result-file-1",     label = "result-file-1@0000004")
  val `commit4-result-file2`: Node =      node(id = "file:///blob/0000004/result-file-2",     label = "result-file-2@0000004")
  // format: on

  private def node(id: String, label: String): Node = Node(NodeId(id), NodeLabel(label))

  def sourceNode(node: Node): SourceNode = SourceNode(node.id, node.label)
  def targetNode(node: Node): TargetNode = TargetNode(node.id, node.label)
}

class TestData(renkuBaseUrl: RenkuBaseUrl) {

  def triples(projectPath: ProjectPath): String =
    s"""
       |<rdf:RDF
       |   xmlns:prov="http://www.w3.org/ns/prov#"
       |   xmlns:dcterms="http://purl.org/dc/terms/"
       |   xmlns:foaf="http://xmlns.com/foaf/0.1/"
       |   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
       |   xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#">
       |  <rdf:Description rdf:about="file:///commit/0000001">
       |    <rdfs:label>0000001</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:comment>renku dataset add zhbikes https://data.stadt-zuerich.ch/dataset/verkehrszaehlungen_werte_fussgaenger_velo/resource/d17a0a74-1073-46f0-a26e-46a403c061ec/download/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv</rdfs:comment>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000004/outputs/output_1">
       |    <prov:activity rdf:resource="file:///commit/0000004"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000004">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
       |    <prov:qualifiedAssociation rdf:resource="file:///commit/0000004/association"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#ProcessRun"/>
       |    <rdfs:comment>renku run python source-file-2 preprocessed-data</rdfs:comment>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:label>.renku/workflow/5e5ac7d7efcc4d829da8b19f9b900a11_python.cwl@0000004</rdfs:label>
       |    <prov:qualifiedUsage rdf:resource="file:///commit/0000004/inputs/input_2"/>
       |    <prov:qualifiedUsage rdf:resource="file:///commit/0000004/inputs/input_1"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000004/.renku/workflow/5e5ac7d7efcc4d829da8b19f9b900a11_python.cwl">
       |    <rdfs:label>.renku/workflow/5e5ac7d7efcc4d829da8b19f9b900a11_python.cwl@0000004</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Plan"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfdesc#Process"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000001/tree/.gitattributes">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
       |    <prov:activity rdf:resource="file:///commit/0000001"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000004/inputs/input_2">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Usage"/>
       |    <prov:entity rdf:resource="file:///blob/0000003/preprocessed-data"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000003/association">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Association"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000003/inputs/input_2">
       |    <prov:entity rdf:resource="file:///blob/0000001/input-data"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Usage"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000002/src">
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:label>src@0000002</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000004/association">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Association"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000002">
       |    <rdfs:comment>added refactored scripts</rdfs:comment>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:label>0000002</rdfs:label>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000001/tree/input-data/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv">
       |    <prov:activity rdf:resource="file:///commit/0000001"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000003">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Activity"/>
       |    <rdfs:label>.renku/workflow/84ff4b53ae634b589f2e83e7c36bd45a_python.cwl@0000003</rdfs:label>
       |    <prov:qualifiedAssociation rdf:resource="file:///commit/0000003/association"/>
       |    <prov:qualifiedUsage rdf:resource="file:///commit/0000003/inputs/input_1"/>
       |    <prov:qualifiedUsage rdf:resource="file:///commit/0000003/inputs/input_2"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#ProcessRun"/>
       |    <rdfs:comment>renku run python source-file-1 input-data preprocessed-data</rdfs:comment>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000001/tree/.renku/refs/datasets/zhbikes">
       |    <prov:activity rdf:resource="file:///commit/0000001"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml">
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <prov:qualifiedGeneration rdf:resource="file:///commit/0000001/tree/.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml"/>
       |    <rdfs:label>.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml@0000001</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/.renku/datasets/f0d5e338c7644f1995484ac00108d525">
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
       |    <rdfs:label>.renku/datasets/f0d5e338c7644f1995484ac00108d525@0000001</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/data">
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <rdfs:label>data@0000001</rdfs:label>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000002/tree/source-file-2">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
       |    <prov:activity rdf:resource="file:///commit/0000002"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/.renku/refs/datasets">
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:label>.renku/refs/datasets@0000001</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000002/source-file-1">
       |    <rdfs:label>source-file-1@0000002</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <prov:qualifiedGeneration rdf:resource="file:///commit/0000002/tree/source-file-1"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/.renku/refs">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:label>.renku/refs@0000001</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000003/preprocessed-data">
       |    <prov:qualifiedGeneration rdf:resource="file:///commit/0000003/outputs/output_0"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:label>preprocessed-data@0000003</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/.renku">
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:label>.renku@0000001</rdfs:label>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000003/outputs/output_0">
       |    <prov:activity rdf:resource="file:///commit/0000003"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000004/result-file-1">
       |    <rdfs:label>result-file-1@0000004</rdfs:label>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <prov:qualifiedGeneration rdf:resource="file:///commit/0000004/outputs/output_1"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/input-data/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv">
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <prov:qualifiedGeneration rdf:resource="file:///commit/0000001/tree/input-data/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <rdfs:label>input-data/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv@0000001</rdfs:label>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/input-data">
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <rdfs:label>input-data@0000001</rdfs:label>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000003/inputs/input_1">
       |    <prov:entity rdf:resource="file:///blob/0000002/source-file-1"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Usage"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/.gitattributes">
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <prov:qualifiedGeneration rdf:resource="file:///commit/0000001/tree/.gitattributes"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <rdfs:label>.gitattributes@0000001</rdfs:label>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000002/source-file-2">
       |    <rdfs:label>source-file-2@0000002</rdfs:label>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <prov:qualifiedGeneration rdf:resource="file:///commit/0000002/tree/source-file-2"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000004/result-file-2">
       |    <rdfs:label>result-file-2@0000004</rdfs:label>
       |    <prov:qualifiedGeneration rdf:resource="file:///commit/0000004/outputs/output_0"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/.renku/refs/datasets/zhbikes">
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:label>.renku/refs/datasets/zhbikes@0000001</rdfs:label>
       |    <prov:qualifiedGeneration rdf:resource="file:///commit/0000001/tree/.renku/refs/datasets/zhbikes"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000001/.renku/datasets">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdfs:label>.renku/datasets@0000001</rdfs:label>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Collection"/>
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfprov#Artifact"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///blob/0000003/.renku/workflow/84ff4b53ae634b589f2e83e7c36bd45a_python.cwl">
       |    <rdf:type rdf:resource="http://purl.org/wf4ever/wfdesc#Process"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Entity"/>
       |    <dcterms:isPartOf rdf:resource="${renkuBaseUrl / projectPath}"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Plan"/>
       |    <rdfs:label>.renku/workflow/84ff4b53ae634b589f2e83e7c36bd45a_python.cwl@0000003</rdfs:label>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000004/outputs/output_0">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
       |    <prov:activity rdf:resource="file:///commit/0000004"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000002/tree/source-file-1">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
       |    <prov:activity rdf:resource="file:///commit/0000002"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000001/tree/.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml">
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Generation"/>
       |    <prov:activity rdf:resource="file:///commit/0000001"/>
       |  </rdf:Description>
       |  <rdf:Description rdf:about="file:///commit/0000004/inputs/input_1">
       |    <prov:entity rdf:resource="file:///blob/0000002/source-file-2"/>
       |    <rdf:type rdf:resource="http://www.w3.org/ns/prov#Usage"/>
       |  </rdf:Description>
       |</rdf:RDF>""".stripMargin
}
