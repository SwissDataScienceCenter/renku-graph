/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets

import ch.datascience.graph.model.datasets.{DerivedFrom, IdSameAs, SameAs, UrlSameAs}
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery
import ch.datascience.tinytypes.Renderer
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.CuratedTriples.Update
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.datasets.TopmostDataFinder.TopmostData
import eu.timepit.refined.auto._
import io.renku.jsonld.EntityId

private class UpdatesCreator {

  def prepareUpdates(topmostData: TopmostData): List[Update] =
    prepareSameAsUpdates(topmostData.entityId, topmostData.sameAs) ++:
      prepareDerivedFromUpdates(
        topmostData.entityId,
        topmostData.derivedFrom
      )

  private def prepareSameAsUpdates(entityId: EntityId, topmostSameAs: SameAs): List[Update] = List(
    Update(
      s"Updating Dataset $entityId topmostSameAs",
      SparqlQuery(
        "upload - topmostSameAs update",
        Set(
          "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
        ),
        s"""|DELETE { ${entityId.asRdfResource} renku:topmostSameAs ?topmostSameAs }
            |INSERT { ${entityId.asRdfResource} renku:topmostSameAs ${topmostSameAs.showAs[RdfResource]} }
            |WHERE {
            |  OPTIONAL { ${entityId.asRdfResource} renku:topmostSameAs ?maybeSameAs }
            |  BIND (IF(BOUND(?maybeSameAs), ?maybeSameAs, "nonexisting") AS ?topmostSameAs)
            |}
            |""".stripMargin
      )
    )
  )

  private def prepareDerivedFromUpdates(entityId: EntityId, topmostDerivedFrom: DerivedFrom): List[Update] = List(
    Update(
      s"Updating Dataset $entityId topmostDerivedFrom",
      SparqlQuery(
        "upload - topmostDerivedFrom update",
        Set(
          "PREFIX renku: <https://swissdatasciencecenter.github.io/renku-ontology#>"
        ),
        s"""|DELETE { ${entityId.asRdfResource} renku:topmostDerivedFrom ?topmostDerivedFrom }
            |INSERT { ${entityId.asRdfResource} renku:topmostDerivedFrom ${topmostDerivedFrom.showAs[RdfResource]} }
            |WHERE {
            |  OPTIONAL { ${entityId.asRdfResource} renku:topmostDerivedFrom ?maybeDerivedFrom }
            |  BIND (IF(BOUND(?maybeDerivedFrom), ?maybeDerivedFrom, "nonexisting") AS ?topmostDerivedFrom)
            |}
            |""".stripMargin
      )
    )
  )

  private implicit class EntityIdOps(entityId: EntityId) {
    lazy val asRdfResource: String = s"<$entityId>"
  }

  private implicit val sameAsRdfRenderer: Renderer[RdfResource, SameAs] = {
    case value: IdSameAs  => s"<$value>"
    case value: UrlSameAs => value.toString
  }

  private implicit val derivedFromRdfRenderer: Renderer[RdfResource, DerivedFrom] =
    (value: DerivedFrom) => s"<$value>"
}
