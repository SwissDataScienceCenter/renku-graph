/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.rdfstore.entities

import cats.syntax.all._
import ch.datascience.graph.config.{GitLabApiUrl, RenkuBaseUrl}
import ch.datascience.graph.model.events.{CommitId, CommittedDate}
import ch.datascience.rdfstore.FusekiBaseUrl
import ch.datascience.rdfstore.entities.ProcessRun.{ChildProcessRun, StandAloneProcessRun, WorkflowProcessRun}
import ch.datascience.rdfstore.entities.WorkflowRun.ActivityWorkflowRun

import scala.reflect.ClassTag

class Activity(val commitId:                 CommitId,
               val committedDate:            CommittedDate,
               val committer:                Person,
               val project:                  Project,
               val agent:                    Agent,
               val comment:                  String,
               val maybeInformedBy:          Option[Activity],
               val maybeInfluenced:          Option[Activity],
               val maybeInvalidation:        Option[Entity with Artifact],
               val maybeGenerationFactories: List[Activity => Generation]
) {
  lazy val generations: List[Generation] = maybeGenerationFactories.map(_.apply(this))

  def entity(location: Location): Entity with Artifact =
    generations
      .flatMap(_.maybeReverseEntity)
      .find(_.location == location)
      .getOrElse(throw new IllegalStateException(s"No entity for $location on Activity for $commitId"))

  def entity[T](implicit tag: ClassTag[T]): T =
    generations
      .flatMap(_.maybeReverseEntity)
      .find(entity => tag.runtimeClass isAssignableFrom entity.getClass)
      .getOrElse(throw new IllegalStateException(s"No entity of type ${tag.runtimeClass} on Activity for $commitId"))
      .asInstanceOf[T]

  def copy(committer: Person, project: Project): Activity = Activity(commitId,
                                                                     committedDate,
                                                                     committer,
                                                                     project,
                                                                     agent,
                                                                     comment,
                                                                     maybeInformedBy,
                                                                     maybeInfluenced,
                                                                     maybeInvalidation,
                                                                     maybeGenerationFactories
  )
  def copy(committer: Person): Activity = copy(committer, project)
}

object Activity {

  def apply(id:                       CommitId,
            committedDate:            CommittedDate,
            committer:                Person,
            project:                  Project,
            agent:                    Agent,
            comment:                  String = "some comment",
            maybeInformedBy:          Option[Activity] = None,
            maybeInfluenced:          Option[Activity] = None,
            maybeInvalidation:        Option[Entity with Artifact] = None,
            maybeGenerationFactories: List[Activity => Generation] = Nil
  ): Activity =
    new Activity(id,
                 committedDate,
                 committer,
                 project,
                 agent,
                 comment,
                 maybeInformedBy,
                 maybeInfluenced,
                 maybeInvalidation,
                 maybeGenerationFactories
    )

  import io.renku.jsonld._
  import io.renku.jsonld.syntax._

  private[entities] implicit def converter(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): PartialEntityConverter[Activity] =
    new PartialEntityConverter[Activity] {
      override def convert[T <: Activity]: T => Either[Exception, PartialEntity] =
        entity =>
          for {
            reverseInvalidation <- entity.maybeInvalidation match {
                                     case Some(invalidation) =>
                                       Reverse.of((prov / "wasInvalidatedBy") -> invalidation.asJsonLD)
                                     case _ => Reverse.empty.asRight[Exception]
                                   }
            reverseGeneration <- Reverse.of((prov / "activity") -> entity.generations.map(_.asJsonLD))
          } yield PartialEntity(
            EntityTypes of (prov / "Activity"),
            reverseInvalidation combine reverseGeneration,
            rdfs / "label"             -> entity.commitId.asJsonLD,
            rdfs / "comment"           -> entity.comment.asJsonLD,
            prov / "startedAtTime"     -> entity.committedDate.asJsonLD,
            prov / "endedAtTime"       -> entity.committedDate.asJsonLD,
            prov / "wasInformedBy"     -> entity.maybeInformedBy.map(_.asEntityId).asJsonLD,
            prov / "wasAssociatedWith" -> JsonLD.arr(entity.agent.asJsonLD, entity.committer.asJsonLD),
            prov / "influenced"        -> entity.maybeInfluenced.asJsonLD,
            schema / "isPartOf"        -> entity.project.asJsonLD
          )

      override def toEntityId: Activity => Option[EntityId] =
        entity => (EntityId of (fusekiBaseUrl / "activities" / "commit" / entity.commitId)).some
    }

  implicit def encoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): JsonLDEncoder[Activity] =
    JsonLDEncoder.instance {
      case a: ActivityWorkflowRun                => a.asJsonLD
      case a: Activity with ChildProcessRun      => a.asJsonLD
      case a: Activity with WorkflowProcessRun   => a.asJsonLD
      case a: Activity with StandAloneProcessRun => a.asJsonLD
      case a: Activity                           => a.asPartialJsonLD[Activity] getOrFail
      case a => throw new Exception(s"Cannot serialize ${a.getClass} Activity")
    }

  implicit def entityIdEncoder(implicit
      renkuBaseUrl:  RenkuBaseUrl,
      gitLabApiUrl:  GitLabApiUrl,
      fusekiBaseUrl: FusekiBaseUrl
  ): EntityIdEncoder[Activity] =
    EntityIdEncoder.instance {
      case a: ActivityWorkflowRun              => a.asEntityId
      case a: Activity with ChildProcessRun    => a.asEntityId
      case a: Activity with WorkflowProcessRun => a.asEntityId
      case a: Activity with StandAloneProcessRun => a.asEntityId
      case a: Activity =>
        converter.toEntityId(a).getOrElse(throw new IllegalStateException(s"No EntityId found for $a"))
      case a => throw new Exception(s"Cannot serialize ${a.getClass} Activity")
    }
}
