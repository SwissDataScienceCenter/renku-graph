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

package ch.datascience.rdfstore.triples

import ch.datascience.generators.CommonGraphGenerators.{emails, names, schemaVersions}
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.EventsGenerators.committedDates
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.{FilePath, ProjectPath}
import ch.datascience.graph.model.users.Email
import ch.datascience.graph.model.{SchemaVersion, projects, users}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.triples.entities._
import ch.datascience.tinytypes.constraints.NonBlank
import ch.datascience.tinytypes.{StringTinyType, TinyTypeFactory}
import eu.timepit.refined.auto._
import io.circe.Json
import org.scalacheck.Gen

object multiFileAndCommit {

  case class MultiFileAndCommitData()(implicit fusekiBaseUrl: FusekiBaseUrl) {
    // format: off
    val resultFile1: FilePath = FilePath("result-file-1")
    private[multiFileAndCommit] val commit1Id = CommitId("0000001")
    private[multiFileAndCommit] val commit2Id = CommitId("0000002")
    private[multiFileAndCommit] val commit3Id = CommitId("0000003")
    val commit4Id: CommitId = CommitId("0000004")
    private[multiFileAndCommit] val commit1DatasetGenerationId         = GenerationActivity.Id(commit1Id, FilePath("tree/.renku/refs/datasets/zhbikes"))
    private[multiFileAndCommit] val commit1ActivityId                  = CommitActivity.Id(commit1Id)
    private[multiFileAndCommit] val commit1CollectionEntityId          = CommitCollectionEntity.Id(commit1Id, FilePath("input-data"))
    private[multiFileAndCommit] val commit2ActivityId                  = CommitActivity.Id(commit2Id)
    private[multiFileAndCommit] val commit2Source1GenerationId         = GenerationArtifact.Id(commit2Id, FilePath("source-file-1"))
    private[multiFileAndCommit] val commit2Source2GenerationId         = GenerationArtifact.Id(commit2Id, FilePath("source-file-2"))
    private[multiFileAndCommit] val commit2Source1GenerationActivityId = GenerationActivity.Id(commit2Id, FilePath("tree/source-file-1"))
    private[multiFileAndCommit] val commit2Source2GenerationActivityId = GenerationActivity.Id(commit2Id, FilePath("tree/source-file-2"))
    private[multiFileAndCommit] val commit3ActivityId                  = CommitActivity.Id(commit3Id)
    private[multiFileAndCommit] val commit3PreprocessedGenerationId    = GenerationArtifact.Id(commit3Id, FilePath("preprocessed-data"))
    private[multiFileAndCommit] val commit3Output0GenerationId         = CommitGeneration.Id(commit3Id, FilePath("outputs/output_0"))
    private[multiFileAndCommit] val commit3AssociationActivityId       = AssociationActivity.Id(commit3Id)
    private[multiFileAndCommit] val commit3Input1UsageEntityId         = UsageEntity.Id(commit3Id, FilePath("inputs/input_1"))
    private[multiFileAndCommit] val commit3Input2UsageEntityId         = UsageEntity.Id(commit3Id, FilePath("inputs/input_2"))
    private[multiFileAndCommit] val commit4AssociationActivityId       = AssociationActivity.Id(commit4Id)
    private[multiFileAndCommit] val commit4Input1UsageEntityId         = UsageEntity.Id(commit4Id, FilePath("inputs/input_1"))
    private[multiFileAndCommit] val commit4Input2UsageEntityId         = UsageEntity.Id(commit4Id, FilePath("inputs/input_2"))
    private[multiFileAndCommit] val commit4Output0GenerationId         = GenerationActivity.Id(commit4Id, FilePath("outputs/output_0"))
    private[multiFileAndCommit] val commit4Output1GenerationId         = GenerationActivity.Id(commit4Id, FilePath("outputs/output_1"))
    private[multiFileAndCommit] val commit4File1GenerationId           = GenerationArtifact.Id(commit4Id, resultFile1)
    private[multiFileAndCommit] val commit4File2GenerationId           = GenerationArtifact.Id(commit4Id, FilePath("result-file-2"))
    val `commit1-input-data`: Resource        = resource(commit1CollectionEntityId,       label = s"${commit1CollectionEntityId.filePath}@$commit1Id")
    val `commit2-source-file1`: Resource      = resource(commit2Source1GenerationId,      label = s"${commit2Source1GenerationId.filePath}@$commit2Id")
    val `commit2-source-file2`: Resource      = resource(commit2Source2GenerationId,      label = s"${commit2Source2GenerationId.filePath}@$commit2Id")
    val `commit3-renku-run`: Resource         = resource(commit3ActivityId,               label = s"renku run python source-file-1 input-data preprocessed-data")
    val `commit3-preprocessed-data`: Resource = resource(commit3PreprocessedGenerationId, label = s"${commit3PreprocessedGenerationId.filePath}@$commit3Id")
    val `commit4-renku-run`: Resource         = resource(commit4AssociationActivityId,    label = s"renku run python source-file-2 preprocessed-data")
    val `commit4-result-file1`: Resource      = resource(commit4File1GenerationId,        label = s"${commit4File1GenerationId.filePath}@$commit4Id")
    val `commit4-result-file2`: Resource      = resource(commit4File2GenerationId,        label = s"${commit4File2GenerationId.filePath}@$commit4Id")
    private def resource(id: EntityId, label: String) = Resource(ResourceName(id.toString), ResourceLabel(label))
    // format: on
  }

  def apply(
      projectPath:          ProjectPath,
      projectName:          projects.Name = projectNames.generateOne,
      projectDateCreated:   projects.DateCreated = projectCreatedDates.generateOne,
      projectCreator:       (users.Name, Option[Email]) = names.generateOne -> Gen.option(emails).generateOne,
      data:                 MultiFileAndCommitData,
      schemaVersion:        SchemaVersion = schemaVersions.generateOne
  )(implicit fusekiBaseUrl: FusekiBaseUrl): List[Json] = {
    val agentId                                        = Agent.Id(schemaVersion)
    val projectId                                      = Project.Id(renkuBaseUrl, projectPath)
    val (projectCreatorName, maybeProjectCreatorEmail) = projectCreator
    val projectCreatorId                               = Person.Id(projectCreatorName)
    import data._

    // format: off
    List(
      Project(projectId, projectName, projectDateCreated, projectCreatorId),
      Agent(schemaVersion),
      Person(projectCreatorId, maybeProjectCreatorEmail),

      CommitActivity(commit1ActivityId, projectId, committedDates.generateOne, Some(agentId), comment = "renku dataset add zhbikes external.csv"),
      GenerationActivity(commit1Id, FilePath("tree/input-data/external.csv"), commit1ActivityId),
      GenerationActivity(commit1DatasetGenerationId, commit1ActivityId),
      GenerationArtifact(commit1Id, FilePath(".renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml"), CommitGeneration.Id(commit1Id, FilePath("tree/.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml")), projectId),
      CommitCollectionEntity(commit1Id, FilePath(".renku/datasets"), projectId),
      CommitCollectionEntity(commit1Id, FilePath(".renku/datasets/f0d5e338c7644f1995484ac00108d525"), projectId),
      CommitCollectionEntity(commit1Id, FilePath("data"), projectId),
      CommitCollectionEntity(commit1Id, FilePath(".renku"), projectId),
      CommitCollectionEntity(commit1Id, FilePath(".renku/refs"), projectId),
      CommitCollectionEntity(commit1Id, FilePath(".renku/refs/datasets"), projectId),
      CommitCollectionEntity(commit1CollectionEntityId, projectId),
      GenerationArtifact(commit1Id, FilePath("input-data/external.csv"), CommitGeneration.Id(commit1Id, FilePath("tree/input-data/external.csv")), projectId),
      GenerationArtifact(commit1Id, FilePath(".renku/refs/datasets/zhbikes"), commit1DatasetGenerationId, projectId),
      GenerationActivity(commit1Id, FilePath("tree/.renku/datasets/f0d5e338c7644f1995484ac00108d525/metadata.yml"), commit1ActivityId),

      CommitActivity(commit2Id, projectId, committedDates.generateOne, Some(agentId), maybePersonId = None, maybeInfluencedBy = Nil, comment = "added refactored scripts"),
      CommitCollectionEntity(commit2Id, FilePath("src"), projectId),
      GenerationActivity(commit2Source2GenerationActivityId, commit2ActivityId),
      GenerationArtifact(commit2Source1GenerationId, commit2Source1GenerationActivityId, projectId),
      GenerationArtifact(commit2Source2GenerationId, commit2Source2GenerationActivityId, projectId),
      GenerationActivity(commit2Source1GenerationActivityId, commit2ActivityId),

      Association(Association.Id(commit3Id)),
      UsageEntity(commit3Input2UsageEntityId, CommitCollectionEntity.Id(commit1Id, FilePath("input-data"))),
      AssociationActivity(commit3AssociationActivityId, projectId, `commit3-renku-run`.label.toString, List(commit3Input1UsageEntityId, commit3Input2UsageEntityId), Some(agentId)),
      GenerationArtifact(commit3PreprocessedGenerationId, commit3Output0GenerationId, projectId),
      CommitGeneration(commit3Output0GenerationId, commit3ActivityId),
      UsageEntity(commit3Input1UsageEntityId, commit2Source1GenerationId),
      ProcessEntity(commit3Id, FilePath(".renku/workflow/84ff4b53ae634b589f2e83e7c36bd45a_python.cwl"), projectId),

      Association(Association.Id(commit4Id)),
      GenerationActivity(commit4Output1GenerationId, commit4AssociationActivityId),
      AssociationActivity(commit4AssociationActivityId, projectId, `commit4-renku-run`.label.toString, List(commit4Input1UsageEntityId, commit4Input2UsageEntityId), Some(agentId)),
      UsageEntity(commit4Input2UsageEntityId, commit3PreprocessedGenerationId),
      GenerationArtifact(commit4File1GenerationId, commit4Output1GenerationId, projectId),
      GenerationArtifact(commit4File2GenerationId, commit4Output0GenerationId, projectId),
      GenerationActivity(commit4Output0GenerationId, commit4AssociationActivityId),
      UsageEntity(commit4Input1UsageEntityId, commit2Source2GenerationId)
    )
  }
  // format: on

  final class ResourceName private (val value: String) extends StringTinyType
  implicit object ResourceName extends TinyTypeFactory[ResourceName](new ResourceName(_)) with NonBlank
  final class ResourceLabel private (val value: String) extends StringTinyType
  implicit object ResourceLabel extends TinyTypeFactory[ResourceLabel](new ResourceLabel(_)) with NonBlank
  case class Resource(name: ResourceName, label: ResourceLabel)
}
