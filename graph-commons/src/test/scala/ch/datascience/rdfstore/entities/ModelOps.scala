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

import ch.datascience.generators.Generators
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.sentenceContaining
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.GraphModelGenerators.datasetIdentifiers
import ch.datascience.graph.model.datasets.{DateCreated, DatePublished, Description, InternalSameAs, Keyword, Name, SameAs, Title}
import ch.datascience.graph.model.{datasets, users}
import ch.datascience.rdfstore.entities.Dataset.Provenance.{ImportedExternal, ImportedInternalAncestorExternal, Internal}
import ch.datascience.rdfstore.entities.ModelOps.DatasetForkingResult
import ch.datascience.rdfstore.entities.Project.ForksCount
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId
import org.scalacheck.Gen

trait ModelOps {

  implicit class ProjectOps[FC <: ForksCount](project: Project[FC])(implicit renkuBaseUrl: RenkuBaseUrl) {

    def fork(): (Project[ForksCount.NonZero], Gen[Project[ForksCount.Zero] with HavingParent]) = {
      val parentProject = incrementForksCount()
      parentProject -> projectEntities[ForksCount.Zero](parentProject.dateCreated).map(child =>
        new Project(
          child.path,
          child.name,
          child.agent,
          child.dateCreated,
          child.maybeCreator,
          child.visibility,
          child.forksCount,
          child.members,
          child.version
        ) with HavingParent {
          override val parent: Project[ForksCount.NonZero] = parentProject
        }
      )
    }

    private def incrementForksCount(): Project[ForksCount.NonZero] =
      project.copy(forksCount = Project.ForksCount(Refined.unsafeApply(project.forksCount.value + 1)))
  }

  implicit class DatasetOps[P <: Dataset.Provenance](dataset: Dataset[P])(implicit renkuBaseUrl: RenkuBaseUrl) {

    lazy val identifier: datasets.Identifier = dataset.identification.identifier

    def forkProject(): DatasetForkingResult[P] = {
      val (updatedOriginalProject, forkProjectGen) = dataset.project.fork()
      DatasetForkingResult(
        dataset.copy(project = updatedOriginalProject),
        dataset.copy(project = forkProjectGen.generateOne)
      )
    }

    def importTo[POUT <: Dataset.Provenance](
        project:              Project[Project.ForksCount]
    )(implicit newProvenance: (EntityId, InternalSameAs, P) => POUT): Dataset[POUT] = {
      val intermediate = dataset.copy(
        project = project,
        identification = dataset.identification.copy(identifier = datasetIdentifiers.generateOne)
      )
      intermediate.copy(
        provenance = newProvenance(intermediate.entityId, SameAs(dataset.entityId), dataset.provenance)
      )
    }

    def to[T](implicit convert: Dataset[P] => T): T = convert(dataset)

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

  implicit class DatasetOpsImportedExternal(dataset: Dataset[ImportedExternal])(implicit renkuBaseUrl: RenkuBaseUrl) {
    def changePublishedDateTo(date: DatePublished): Dataset[ImportedExternal] =
      dataset.copy(provenance = dataset.provenance.copy(date = date))
  }

  implicit class DatasetOpsImportedInternalAncestorExternal(dataset: Dataset[ImportedInternalAncestorExternal])(implicit
      renkuBaseUrl:                                                  RenkuBaseUrl
  ) {
    def changePublishedDateTo(date: DatePublished): Dataset[ImportedInternalAncestorExternal] =
      dataset.copy(provenance = dataset.provenance.copy(date = date))

  }

  implicit class DatasetOpsInternal(dataset: Dataset[Internal])(implicit renkuBaseUrl: RenkuBaseUrl) {
    def changeCreatedDateTo(date: DateCreated): Dataset[Internal] =
      dataset.copy(provenance = dataset.provenance.copy(date = date))
  }

  implicit def importFromInternal(
      newEntityId:    EntityId,
      sameAsToParent: InternalSameAs,
      provenance:     Dataset.Provenance.Internal
  ): Dataset.Provenance.ImportedInternalAncestorInternal =
    Dataset.Provenance.ImportedInternalAncestorInternal(newEntityId,
                                                        sameAsToParent,
                                                        provenance.topmostSameAs,
                                                        provenance.date,
                                                        provenance.creators
    )

  implicit def importFromImportedExternal(
      newEntityId:    EntityId,
      sameAsToParent: InternalSameAs,
      provenance:     Dataset.Provenance.ImportedExternal
  ): Dataset.Provenance.ImportedInternalAncestorExternal =
    Dataset.Provenance.ImportedInternalAncestorExternal(newEntityId,
                                                        sameAsToParent,
                                                        provenance.topmostSameAs,
                                                        provenance.date,
                                                        provenance.creators
    )

  implicit def importFromImportedInternalAncestorExternal(
      newEntityId:    EntityId,
      sameAsToParent: InternalSameAs,
      provenance:     Dataset.Provenance.ImportedInternalAncestorExternal
  ): Dataset.Provenance.ImportedInternalAncestorExternal =
    Dataset.Provenance.ImportedInternalAncestorExternal(newEntityId,
                                                        sameAsToParent,
                                                        provenance.topmostSameAs,
                                                        provenance.date,
                                                        provenance.creators
    )

  implicit def importFromImportedInternalAncestorInternal(
      newEntityId:    EntityId,
      sameAsToParent: InternalSameAs,
      provenance:     Dataset.Provenance.ImportedInternalAncestorInternal
  ): Dataset.Provenance.ImportedInternalAncestorInternal =
    Dataset.Provenance.ImportedInternalAncestorInternal(newEntityId,
                                                        sameAsToParent,
                                                        provenance.topmostSameAs,
                                                        provenance.date,
                                                        provenance.creators
    )

  implicit def importFromModified(
      newEntityId:    EntityId,
      sameAsToParent: InternalSameAs,
      provenance:     Dataset.Provenance.Modified
  ): Dataset.Provenance.ImportedInternalAncestorInternal =
    Dataset.Provenance.ImportedInternalAncestorInternal(
      newEntityId,
      sameAsToParent,
      provenance.topmostSameAs,
      provenance.date,
      provenance.creators
    )

  implicit val creatorUsernameUpdaterInternal
      : (users.Name, Dataset.Provenance.Internal) => Dataset.Provenance.Internal = { case (userName, prov) =>
    prov.copy(creators = prov.creators + persons.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterImportedInternalAncestorInternal
      : (users.Name,
         Dataset.Provenance.ImportedInternalAncestorInternal
      ) => Dataset.Provenance.ImportedInternalAncestorInternal = { case (userName, prov) =>
    prov.copy(creators = prov.creators + persons.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterImportedInternalAncestorExternal
      : (users.Name,
         Dataset.Provenance.ImportedInternalAncestorExternal
      ) => Dataset.Provenance.ImportedInternalAncestorExternal = { case (userName, prov) =>
    prov.copy(creators = prov.creators + persons.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterImportedExternal
      : (users.Name, Dataset.Provenance.ImportedExternal) => Dataset.Provenance.ImportedExternal = {
    case (userName, prov) =>
      prov.copy(creators = prov.creators + persons.generateOne.copy(name = userName))
  }

  implicit val creatorUsernameUpdaterModified
      : (users.Name, Dataset.Provenance.Modified) => Dataset.Provenance.Modified = { case (userName, prov) =>
    prov.copy(creators = prov.creators + persons.generateOne.copy(name = userName))
  }
}

object ModelOps extends ModelOps {
  final case class DatasetForkingResult[DP <: Dataset.Provenance](original: Dataset[DP], fork: Dataset[DP])
}
