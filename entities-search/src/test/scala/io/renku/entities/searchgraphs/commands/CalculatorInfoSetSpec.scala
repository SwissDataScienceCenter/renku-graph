package io.renku.entities.searchgraphs
package commands

import CalculatorInfoSetGenerators._
import Generators._
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators.projectResourceIds
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CalculatorInfoSetSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  "show" should {

    "return String containing project id and path along with model and TS search info" in {
      forAll(calculatorInfoSets) { someInfoSet =>
        val maybeModelInfo = searchInfoObjectsGen(withLinkFor = someInfoSet.project.resourceId).generateOption

        val maybeTsInfo = searchInfoObjectsGen(withLinkFor = someInfoSet.project.resourceId,
                                               and = projectResourceIds.generateList(): _*
        ).generateOption

        val infoSet = someInfoSet.copy(maybeModelInfo = maybeModelInfo, maybeTSInfo = maybeTsInfo)

        infoSet.show shouldBe List(
          show"projectId = ${infoSet.project.resourceId}".some,
          show"projectPath = ${infoSet.project.path}".some,
          infoSet.maybeModelInfo.map(mi => show"modelInfo = [${searchIntoToString(mi)}]"),
          infoSet.maybeTSInfo.map(tsi => show"tsInfo = [${searchIntoToString(tsi)}]")
        ).flatten.mkString(", ")
      }
    }
  }

  private def searchIntoToString(info: SearchInfo) = List(
    show"topmostSameAs = ${info.topmostSameAs}",
    show"name = ${info.name}",
    show"visibility = ${info.visibility}",
    show"links = [${info.links.map(link => show"projectId = ${link.projectId}, datasetId = ${link.datasetId}").intercalate("; ")}}]"
  ).mkString(", ")
}
