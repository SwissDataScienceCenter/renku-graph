package io.renku.entities.searchgraphs
package commands

import Generators._
import UpdateCommand._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.Schemas.{rdf, renku}
import io.renku.graph.model.{GraphClass, datasets}
import io.renku.graph.model.entities.{Dataset, Person, Project}
import io.renku.graph.model.images.Image
import io.renku.triplesstore.Quad
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import QuadsEncoders._

class CommandCalculatorSpec extends AnyWordSpec with should.Matchers {
  import CommandCalculator._

  "calculateCommand" should {

    "create Inserts for all properties when no DS in the TS" in {

      val projectInfo = projectSearchInfoObjects.generateOne

      calculateCommand(projectInfo -> None) shouldBe List(
        Insert(projectInfo.topmostSameAs, rdf / "type", renku / "DatasetSearchInfo"),
        Insert(projectInfo.topmostSameAs, Dataset.Ontology.name, projectInfo.name),
          Insert(projectInfo.topmostSameAs, Project.Ontology.projectVisibility,       projectInfo.visibility),
        projectInfo.date match {
          case d: datasets.DateCreated =>Insert(projectInfo.topmostSameAs, Dataset.Ontology.dateCreated,             d)
          case d: datasets.DatePublished =>Insert(projectInfo.topmostSameAs, Dataset.Ontology.datePublished,             d)
        },
        projectInfo.creators.flatMap{ pi =>
          Quad(GraphClass.Datasets.id,projectInfo.topmostSameAs, Dataset.Ontology.creator,         pi.resourceId) ::
            pi.toQuads
        },
        projectInfo.keywords.map{ keyword =>
          Insert(projectInfo.topmostSameAs, Dataset.Ontology.keywords,         keyword)
        },
        projectInfo.maybeDescription.map{ desc =>
          Insert(projectInfo.topmostSameAs, Dataset.Ontology.description,         desc)
        },
        projectInfo.images.map{ case Image(resourceId, uri, position) => List(
          Insert(projectInfo.topmostSameAs, Dataset.Ontology.image,         resourceId),
          Insert(resourceId, rdf / "type", Image.Ontology.typeClass.id),
          Insert(resourceId, Image.Ontology.contentUrl, uri),
          Insert(resourceId, Image.Ontology.position, position)
        )
        }, {
          List(
          Insert(projectInfo.topmostSameAs, renku / "link", projectInfo.link.resourceId),
            Insert(projectInfo.link.resourceId, rdf / "type", renku / "DatasetProjectLink"),
          Insert(projectInfo.link.resourceId, renku / "project", projectInfo.link.project),
          Insert(projectInfo.link.resourceId, renku / "dataset", projectInfo.link.dataset)
          )
        }
        )
      )
    }
  }
}
