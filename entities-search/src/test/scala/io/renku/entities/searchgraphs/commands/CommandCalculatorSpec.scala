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

package io.renku.entities.searchgraphs
package commands

import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CommandCalculatorSpec extends AnyWordSpec with should.Matchers {

  "calculateCommand" should {

    "create Inserts for all properties when no DS in the TS" in {

//      val projectInfo = projectSearchInfoObjects.generateOne
//
//      calculateCommand(projectInfo -> None) shouldBe List(
//        Insert(projectInfo.topmostSameAs, rdf / "type", renku / "DatasetSearchInfo"),
//        Insert(projectInfo.topmostSameAs, Dataset.Ontology.name, projectInfo.name),
//          Insert(projectInfo.topmostSameAs, Project.Ontology.projectVisibility,       projectInfo.visibility),
//        projectInfo.date match {
//          case d: datasets.DateCreated =>Insert(projectInfo.topmostSameAs, Dataset.Ontology.dateCreated,             d)
//          case d: datasets.DatePublished =>Insert(projectInfo.topmostSameAs, Dataset.Ontology.datePublished,             d)
//        },
//        projectInfo.creators.flatMap{ pi =>
//          Quad(GraphClass.Datasets.id,projectInfo.topmostSameAs, Dataset.Ontology.creator,         pi.resourceId) ::
//            pi.toQuads
//        },
//        projectInfo.keywords.map{ keyword =>
//          Insert(projectInfo.topmostSameAs, Dataset.Ontology.keywords,         keyword)
//        },
//        projectInfo.maybeDescription.map{ desc =>
//          Insert(projectInfo.topmostSameAs, Dataset.Ontology.description,         desc)
//        },
//        projectInfo.images.map{ case Image(resourceId, uri, position) => List(
//          Insert(projectInfo.topmostSameAs, Dataset.Ontology.image,         resourceId),
//          Insert(resourceId, rdf / "type", Image.Ontology.typeClass.id),
//          Insert(resourceId, Image.Ontology.contentUrl, uri),
//          Insert(resourceId, Image.Ontology.position, position)
//        )
//        }, {
//          List(
//          Insert(projectInfo.topmostSameAs, renku / "link", projectInfo.link.resourceId),
//            Insert(projectInfo.link.resourceId, rdf / "type", renku / "DatasetProjectLink"),
//          Insert(projectInfo.link.resourceId, renku / "project", projectInfo.link.project),
//          Insert(projectInfo.link.resourceId, renku / "dataset", projectInfo.link.dataset)
//          )
//        }
//        )
//      )
    }
  }
}
