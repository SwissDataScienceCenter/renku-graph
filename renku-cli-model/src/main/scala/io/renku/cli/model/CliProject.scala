/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.cli.model

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliProject.ProjectPlan
import io.renku.cli.model.Ontologies.{Prov, Renku, Schema}
import io.renku.graph.model.images.Image
import io.renku.graph.model.plans
import io.renku.graph.model.projects._
import io.renku.graph.model.versions.{CliVersion, SchemaVersion}
import io.renku.jsonld._
import io.renku.jsonld.syntax._

final case class CliProject(
    id:            ResourceId,
    name:          Option[Name],
    description:   Option[Description],
    dateCreated:   DateCreated,
    creator:       Option[CliPerson],
    keywords:      Set[Keyword],
    images:        List[Image],
    plans:         List[ProjectPlan],
    datasets:      List[CliDataset],
    activities:    List[CliActivity],
    agentVersion:  Option[CliVersion],
    schemaVersion: Option[SchemaVersion]
) extends CliModel

object CliProject {

  sealed trait ProjectPlan {
    def resourceId: plans.ResourceId
    def fold[A](
        fa: CliStepPlan => A,
        fb: CliCompositePlan => A,
        fc: CliWorkflowFileStepPlan => A,
        fd: CliWorkflowFileCompositePlan => A
    ): A
  }
  object ProjectPlan {
    final case class Step(plan: CliStepPlan) extends ProjectPlan {
      val resourceId: plans.ResourceId = plan.id
      def fold[A](
          fa: CliStepPlan => A,
          fb: CliCompositePlan => A,
          fc: CliWorkflowFileStepPlan => A,
          fd: CliWorkflowFileCompositePlan => A
      ): A = fa(plan)
    }
    final case class Composite(plan: CliCompositePlan) extends ProjectPlan {
      val resourceId: plans.ResourceId = plan.id
      def fold[A](
          fa: CliStepPlan => A,
          fb: CliCompositePlan => A,
          fc: CliWorkflowFileStepPlan => A,
          fd: CliWorkflowFileCompositePlan => A
      ): A = fb(plan)
    }
    final case class WorkflowFile(plan: CliWorkflowFileStepPlan) extends ProjectPlan {
      val resourceId: plans.ResourceId = plan.id
      def fold[A](
          fa: CliStepPlan => A,
          fb: CliCompositePlan => A,
          fc: CliWorkflowFileStepPlan => A,
          fd: CliWorkflowFileCompositePlan => A
      ): A = fc(plan)
    }
    final case class WorkflowFileComposite(plan: CliWorkflowFileCompositePlan) extends ProjectPlan {
      val resourceId: plans.ResourceId = plan.id
      def fold[A](
          fa: CliStepPlan => A,
          fb: CliCompositePlan => A,
          fc: CliWorkflowFileStepPlan => A,
          fd: CliWorkflowFileCompositePlan => A
      ): A = fd(plan)
    }

    def apply(plan: CliStepPlan):                  ProjectPlan = Step(plan)
    def apply(plan: CliCompositePlan):             ProjectPlan = Composite(plan)
    def apply(plan: CliWorkflowFileStepPlan):      ProjectPlan = WorkflowFile(plan)
    def apply(plan: CliWorkflowFileCompositePlan): ProjectPlan = WorkflowFileComposite(plan)
    def apply(plan: CliPlan):                      ProjectPlan = plan.fold(apply, apply)

    private val entityTypes: EntityTypes = EntityTypes.of(Prov.Plan, Schema.Action, Schema.CreativeWork)

    private def selectCandidates(ets: EntityTypes): Boolean =
      CliStepPlan.matchingEntityTypes(ets) ||
        CliCompositePlan.matchingEntityTypes(ets) ||
        CliWorkflowFileStepPlan.matchingEntityTypes(ets) ||
        CliWorkflowFileCompositePlan.matchingEntityTypes(ets)

    implicit def jsonLDDecoder: JsonLDDecoder[ProjectPlan] =
      JsonLDDecoder.entity(entityTypes, _.getEntityTypes.map(selectCandidates)) { cursor =>
        val currentTypes = cursor.getEntityTypes
        (currentTypes.map(CliStepPlan.matchingEntityTypes),
         currentTypes.map(CliCompositePlan.matchingEntityTypes),
         currentTypes.map(CliWorkflowFileStepPlan.matchingEntityTypes),
         currentTypes.map(CliWorkflowFileCompositePlan.matchingEntityTypes)
        ).flatMapN {
          case (true, _, _, _) =>
            CliStepPlan.jsonLDDecoder.emap(p => Right(ProjectPlan(p)))(cursor)
          case (_, true, _, _) =>
            CliCompositePlan.jsonLDDecoder.emap(p => Right(ProjectPlan(p)))(cursor)
          case (_, _, true, _) =>
            CliWorkflowFileStepPlan.jsonLDDecoder.emap(p => Right(ProjectPlan(p)))(cursor)
          case (_, _, _, true) =>
            CliWorkflowFileCompositePlan.jsonLDDecoder.emap(p => Right(ProjectPlan(p)))(cursor)
          case _ =>
            Left(
              DecodingFailure(
                s"Invalid entity types for decoding a project plan: $currentTypes",
                Nil
              )
            )
        }
      }

    implicit val jsonLDEncoder: JsonLDEncoder[ProjectPlan] =
      JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD, _.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Schema.Project, Prov.Location)

  implicit val jsonLDDecoder: JsonLDEntityDecoder[CliProject] =
    JsonLDDecoder.cacheableEntity(entityTypes) { cursor =>
      for {
        id            <- cursor.downEntityId.as[ResourceId]
        name          <- cursor.downField(Schema.name).as[Option[Name]]
        description   <- cursor.downField(Schema.description).as[Option[Description]]
        dateCreated   <- cursor.downField(Schema.dateCreated).as[DateCreated]
        creator       <- cursor.downField(Schema.creator).as[Option[CliPerson]]
        keywords      <- cursor.downField(Schema.keywords).as[Set[Option[Keyword]]].map(_.flatten)
        images        <- cursor.downField(Schema.image).as[List[Image]].map(_.sortBy(_.position))
        plans         <- cursor.downField(Renku.hasPlan).as[List[ProjectPlan]]
        datasets      <- cursor.downField(Renku.hasDataset).as[List[CliDataset]]
        activities    <- cursor.downField(Renku.hasActivity).as[List[CliActivity]].map(_.sortBy(_.startTime))
        agentVersion  <- cursor.downField(Schema.agent).as[Option[CliVersion]]
        schemaVersion <- cursor.downField(Schema.schemaVersion).as[Option[SchemaVersion]]
      } yield CliProject(
        id,
        name,
        description,
        dateCreated,
        creator,
        keywords,
        images,
        plans,
        datasets,
        activities,
        agentVersion,
        schemaVersion
      )
    }

  /** Decodes a project and also collects all `Person` entities in the given json, potentially 
   * unrelated to the project. 
   */
  val projectAndPersonDecoder: JsonLDDecoder[(CliProject, List[CliPerson])] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      val pr = cursor.as[CliProject]
      val pe = cursor.focusTop
        .as[List[CliPerson]]
        .leftMap(failure =>
          DecodingFailure(s"Finding Person entities for project ${pr.map(_.name)} failed: ${failure.getMessage()}", Nil)
        )
      (pr, pe).mapN(Tuple2.apply)
    }

  implicit def jsonLDEncoder: JsonLDEncoder[CliProject] =
    JsonLDEncoder.instance { project =>
      JsonLD.entity(
        project.id.asEntityId,
        entityTypes,
        Schema.agent         -> project.agentVersion.asJsonLD,
        Schema.creator       -> project.creator.asJsonLD,
        Schema.dateCreated   -> project.dateCreated.asJsonLD,
        Schema.description   -> project.description.asJsonLD,
        Schema.name          -> project.name.asJsonLD,
        Schema.schemaVersion -> project.schemaVersion.asJsonLD,
        Schema.image         -> project.images.asJsonLD,
        Schema.keywords      -> project.keywords.asJsonLD,
        Renku.hasPlan        -> project.plans.asJsonLD,
        Renku.hasDataset     -> project.datasets.asJsonLD,
        Renku.hasActivity    -> project.activities.asJsonLD
      )
    }

  /** Encodes into a flat array, including publication events. */
  def flatJsonLDEncoder: JsonLDEncoder[CliProject] =
    JsonLDEncoder.instance { project =>
      val data = project.asNestedJsonLD :: project.datasets.flatMap(_.publicationEvents).map(_.asNestedJsonLD)
      JsonLD.arr(data: _*).flatten.fold(throw _, identity)
    }
}
