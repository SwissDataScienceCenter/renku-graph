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

package ch.datascience.graph.model.testentities

import cats.data.{NonEmptyList, Validated, ValidatedNel}
import cats.syntax.all._
import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.sentenceContaining
import ch.datascience.graph.model.GraphModelGenerators.datasetIdentifiers
import ch.datascience.graph.model.datasets.{DateCreated, DerivedFrom, Description, InitialVersion, InternalSameAs, Keyword, Name, SameAs, Title}
import ch.datascience.graph.model._
import ModelOps.DatasetForkingResult
import Project.ForksCount
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.Positive
import io.renku.jsonld.EntityId
import io.renku.jsonld.syntax._

trait ModelOps {

  implicit class PersonOps(person: Person) {
    lazy val resourceId: users.ResourceId = users.ResourceId(person.asEntityId)

    def to[T](implicit convert:      Person => T):         T         = convert(person)
    def toMaybe[T](implicit convert: Person => Option[T]): Option[T] = convert(person)
  }

  implicit class ProjectOps[FC <: ForksCount](project: Project[FC])(implicit renkuBaseUrl: RenkuBaseUrl) {

    lazy val resourceId: projects.ResourceId = projects.ResourceId(project.asEntityId)

    def to[T](implicit convert: Project[FC] => T): T = convert(project)

    def forkOnce(): (Project[ForksCount.NonZero], ProjectWithParent[ForksCount.Zero]) = {
      val (parent, childGen) = fork(times = 1)
      parent -> childGen.head
    }

    def fork(
        times: Int Refined Positive
    ): (Project[ForksCount.NonZero], NonEmptyList[ProjectWithParent[ForksCount.Zero]]) = {
      val parent = project match {
        case proj: ProjectWithParent[FC] =>
          proj.copy(forksCount = ForksCount(Refined.unsafeApply(proj.forksCount.value + times.value)))
        case proj: ProjectWithoutParent[FC] =>
          proj.copy(forksCount = Project.ForksCount(Refined.unsafeApply(project.forksCount.value + times.value)))
      }
      parent -> (1 to times.value).foldLeft(NonEmptyList.one(newChildGen(parent).generateOne))((childrenGens, _) =>
        newChildGen(parent).generateOne :: childrenGens
      )
    }

    private def newChildGen(parentProject: Project[Project.ForksCount.NonZero]) =
      projectEntities[ForksCount.Zero](fixed(parentProject.visibility), parentProject.dateCreated).map(child =>
        ProjectWithParent(
          child.path,
          child.name,
          child.agent,
          child.dateCreated,
          child.maybeCreator,
          child.visibility,
          child.forksCount,
          child.members,
          child.version,
          parentProject
        )
      )
  }

  implicit class DatasetOps[P <: Dataset.Provenance](dataset: Dataset[P])(implicit renkuBaseUrl: RenkuBaseUrl) {

    lazy val identifier: datasets.Identifier = dataset.identification.identifier

    def to[T](implicit convert: Dataset[P] => T): T = convert(dataset)

    def forkProject(): DatasetForkingResult[P] = {
      val (updatedOriginalProject, forkProject) = dataset.project.forkOnce()
      DatasetForkingResult(
        original = dataset.copy(project = updatedOriginalProject),
        fork = dataset.copy(project = forkProject)
      )
    }

    def importTo[POUT <: Dataset.Provenance](
        project:              Project[Project.ForksCount]
    )(implicit newProvenance: ProvenanceImportFactory[P, POUT]): Dataset[POUT] = {
      val newIdentifier = datasetIdentifiers.generateOne
      dataset.copy(
        identification = dataset.identification.copy(identifier = newIdentifier),
        provenance = newProvenance(
          Dataset.entityId(newIdentifier),
          SameAs(dataset.entityId),
          dataset.provenance,
          InitialVersion(newIdentifier)
        ),
        project = project
      )
    }

    def invalidate(
        time: InvalidationTime
    ): ValidatedNel[String, Dataset[Dataset.Provenance.Modified] with HavingInvalidationTime] = {
      val newIdentifier = datasetIdentifiers.generateOne
      dataset.provenance.date match {
        case dateCreated: DateCreated =>
          Validated.condNel(
            test = (time.value compareTo dateCreated.instant) >= 0,
            new Dataset(dataset.identification.copy(identifier = newIdentifier),
                        Dataset.Provenance.Modified(
                          Dataset.entityId(newIdentifier),
                          DerivedFrom(dataset.entityId),
                          dataset.provenance.topmostDerivedFrom,
                          dataset.provenance.initialVersion,
                          datasets.DateCreated(time.value),
                          dataset.provenance.creators + personEntities.generateOne
                        ),
                        dataset.additionalInfo,
                        dataset.publishing,
                        dataset.parts,
                        dataset.project
            ) with HavingInvalidationTime {
              override val invalidationTime: InvalidationTime = time
            },
            s"Invalidation time $time on dataset with id: ${dataset.identification.identifier} is older than dataset date"
          )
        case _ =>
          Validated.condNel(
            test = (time.value compareTo dataset.project.dateCreated.value) >= 0,
            new Dataset(dataset.identification.copy(identifier = newIdentifier),
                        Dataset.Provenance.Modified(
                          Dataset.entityId(newIdentifier),
                          DerivedFrom(dataset.entityId),
                          dataset.provenance.topmostDerivedFrom,
                          dataset.provenance.initialVersion,
                          datasets.DateCreated(time.value),
                          dataset.provenance.creators + personEntities.generateOne
                        ),
                        dataset.additionalInfo,
                        dataset.publishing,
                        dataset.parts,
                        dataset.project
            ) with HavingInvalidationTime {
              override val invalidationTime: InvalidationTime = time
            },
            s"Invalidation time $time on dataset with id: ${dataset.identification.identifier} is older than the project date"
          )
      }
    }

    def invalidatePart(part: DatasetPart,
                       time: InvalidationTime
    ): ValidatedNel[String, Dataset[Dataset.Provenance.Modified]] =
      dataset.parts
        .find(_ == part)
        .toValidNel(s"There's no part ${part.entity.location} on dataset with id ${dataset.identifier}")
        .andThen(_.invalidate(time))
        .map { invalidatedPart =>
          val newIdentifier = datasetIdentifiers.generateOne
          dataset.copy(
            dataset.identification.copy(identifier = newIdentifier),
            provenance = Dataset.Provenance.Modified(
              Dataset.entityId(newIdentifier),
              DerivedFrom(dataset.entityId),
              dataset.provenance.topmostDerivedFrom,
              dataset.provenance.initialVersion,
              datasets.DateCreated(time.value),
              dataset.provenance.creators + personEntities.generateOne
            ),
            parts = dataset.parts.filterNot(_ == part) ::: invalidatedPart :: Nil
          )
        }

    def makeNameContaining(phrase: String): Dataset[P] = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase)
      dataset.copy(
        identification = dataset.identification.copy(name =
          sentenceContaining(nonEmptyPhrase).map(_.value).map(Name.apply).generateOne
        )
      )
    }

    def makeTitleContaining(phrase: String): Dataset[P] = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase)
      dataset.copy(
        identification = dataset.identification.copy(title =
          sentenceContaining(nonEmptyPhrase).map(_.value).map(Title.apply).generateOne
        )
      )
    }

    def makeCreatorNameContaining(phrase: String)(implicit provenanceUpdater: (users.Name, P) => P): Dataset[P] = {
      val nonEmptyPhrase: Generators.NonBlank = Refined.unsafeApply(phrase)
      dataset.copy(
        provenance =
          provenanceUpdater(sentenceContaining(nonEmptyPhrase).map(_.value).map(users.Name.apply).generateOne,
                            dataset.provenance
          )
      )
    }

    def makeKeywordsContaining(phrase: String): Dataset[P] =
      dataset.copy(
        additionalInfo = dataset.additionalInfo.copy(keywords = dataset.additionalInfo.keywords :+ Keyword(phrase))
      )

    def makeDescContaining(phrase: String): Dataset[P] =
      dataset.copy(additionalInfo =
        dataset.additionalInfo.copy(maybeDescription =
          sentenceContaining(Refined.unsafeApply(phrase)).map(_.value).map(Description.apply).generateSome
        )
      )
  }

  type ProvenanceImportFactory[OldProvenance <: Dataset.Provenance, NewProvenance <: Dataset.Provenance] =
    (EntityId, InternalSameAs, OldProvenance, InitialVersion) => NewProvenance

  implicit lazy val fromInternalToImportedInternalAncestorInternal
      : ProvenanceImportFactory[Dataset.Provenance.Internal, Dataset.Provenance.ImportedInternalAncestorInternal] =
    (newEntityId, sameAs, oldProvenance, initialVersion) =>
      Dataset.Provenance.ImportedInternalAncestorInternal(newEntityId,
                                                          sameAs,
                                                          oldProvenance.topmostSameAs,
                                                          initialVersion,
                                                          oldProvenance.date,
                                                          oldProvenance.creators
      )

  implicit lazy val fromImportedExternalToImportedInternalAncestorExternal
      : ProvenanceImportFactory[Dataset.Provenance.ImportedExternal,
                                Dataset.Provenance.ImportedInternalAncestorExternal
      ] = (newEntityId, sameAs, oldProvenance, initialVersion) =>
    Dataset.Provenance.ImportedInternalAncestorExternal(newEntityId,
                                                        sameAs,
                                                        oldProvenance.topmostSameAs,
                                                        initialVersion,
                                                        oldProvenance.date,
                                                        oldProvenance.creators
    )

  implicit lazy val fromImportedInternalAncestorExternalToImportedInternalAncestorExternal
      : ProvenanceImportFactory[Dataset.Provenance.ImportedInternalAncestorExternal,
                                Dataset.Provenance.ImportedInternalAncestorExternal
      ] = (newEntityId, sameAs, oldProvenance, initialVersion) =>
    Dataset.Provenance.ImportedInternalAncestorExternal(newEntityId,
                                                        sameAs,
                                                        oldProvenance.topmostSameAs,
                                                        initialVersion,
                                                        oldProvenance.date,
                                                        oldProvenance.creators
    )

  implicit lazy val fromImportedInternalAncestorInternalToImportedInternalAncestorInternal
      : ProvenanceImportFactory[Dataset.Provenance.ImportedInternalAncestorInternal,
                                Dataset.Provenance.ImportedInternalAncestorInternal
      ] = (newEntityId, sameAs, oldProvenance, initialVersion) =>
    Dataset.Provenance.ImportedInternalAncestorInternal(newEntityId,
                                                        sameAs,
                                                        oldProvenance.topmostSameAs,
                                                        initialVersion,
                                                        oldProvenance.date,
                                                        oldProvenance.creators
    )

  implicit lazy val importFromModifiedToImportedInternalAncestorInternal
      : ProvenanceImportFactory[Dataset.Provenance.Modified, Dataset.Provenance.ImportedInternalAncestorInternal] =
    (newEntityId, sameAs, oldProvenance, initialVersion) =>
      Dataset.Provenance.ImportedInternalAncestorInternal(newEntityId,
                                                          sameAs,
                                                          oldProvenance.topmostSameAs,
                                                          initialVersion,
                                                          oldProvenance.date,
                                                          oldProvenance.creators
      )

  implicit val creatorUsernameUpdaterInternal
      : (users.Name, Dataset.Provenance.Internal) => Dataset.Provenance.Internal = { case (userName, prov) =>
    prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterImportedInternalAncestorInternal
      : (users.Name,
         Dataset.Provenance.ImportedInternalAncestorInternal
      ) => Dataset.Provenance.ImportedInternalAncestorInternal = { case (userName, prov) =>
    prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterImportedInternalAncestorExternal
      : (users.Name,
         Dataset.Provenance.ImportedInternalAncestorExternal
      ) => Dataset.Provenance.ImportedInternalAncestorExternal = { case (userName, prov) =>
    prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterImportedExternal
      : (users.Name, Dataset.Provenance.ImportedExternal) => Dataset.Provenance.ImportedExternal = {
    case (userName, prov) =>
      prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterModified
      : (users.Name, Dataset.Provenance.Modified) => Dataset.Provenance.Modified = { case (userName, prov) =>
    prov.copy(creators = prov.creators + personEntities.generateOne.copy(name = userName))
  }

  implicit class DatasetPartOps(part: DatasetPart) {

    private[ModelOps] def invalidate(
        time: InvalidationTime
    ): ValidatedNel[String, DatasetPart with HavingInvalidationTime] =
      Validated.condNel(
        test = (time.value compareTo part.dateCreated.value) >= 0,
        new DatasetPart(datasets.PartId.generate,
                        part.external,
                        part.entity,
                        part.dateCreated,
                        part.maybeUrl,
                        part.maybeSource
        ) with HavingInvalidationTime {
          override val invalidationTime: InvalidationTime = time
        },
        s"Invalidation time $time is older than dataset part ${part.entity.location}"
      )
  }

  implicit class RunPlanOps(runPlan: RunPlan) {

    def invalidate(time: InvalidationTime): ValidatedNel[String, RunPlan with HavingInvalidationTime] =
      Validated.condNel(
        test = (time.value compareTo runPlan.project.dateCreated.value) >= 0,
        new RunPlan(runPlan.id,
                    runPlan.name,
                    runPlan.maybeDescription,
                    runPlan.command,
                    runPlan.maybeProgrammingLanguage,
                    runPlan.keywords,
                    runPlan.commandParameterFactories,
                    runPlan.successCodes,
                    runPlan.project
        ) with HavingInvalidationTime {
          override val invalidationTime: InvalidationTime = time
        },
        s"Invalidation time $time on RunPlan with name: ${runPlan.name} is older than project date"
      )
  }
}

object ModelOps extends ModelOps {
  final case class DatasetForkingResult[DP <: Dataset.Provenance](original: Dataset[DP], fork: Dataset[DP])
}
