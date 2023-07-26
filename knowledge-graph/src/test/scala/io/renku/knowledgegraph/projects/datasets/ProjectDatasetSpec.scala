package io.renku.knowledgegraph.projects.datasets

import Generators._
import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import io.renku.config.renku
import io.renku.generators.CommonGraphGenerators._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GitLabUrl
import io.renku.graph.model.GraphModelGenerators._
import io.renku.knowledgegraph.projects.images.ImagesEncoder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class ProjectDatasetSpec extends AnyFlatSpec with should.Matchers with ScalaCheckPropertyChecks with ImagesEncoder {

  it should "encode to JSON" in {
    forAll(projectDatasetGen) { datasets =>
      datasets.asJson(ProjectDataset.encoder(projectPath)) shouldBe toJson(datasets)
    }
  }

  private lazy val toJson: ProjectDataset => Json = {
    case ProjectDataset(id, originalId, title, name, Left(sameAs), images) =>
      json"""{
        "identifier": $id,
        "versions": {
          "initial": $originalId
        },
        "title":  $title,
        "name":   $name,
        "slug":   $name,
        "sameAs": $sameAs,
        "images": ${images -> projectPath},
        "_links": [{
          "rel":  "details",
          "href": ${renkuApiUrl / "datasets" / id}
        }, {
          "rel":  "initial-version",
          "href": ${renkuApiUrl / "datasets" / originalId}
        }, {
          "rel":  "tags",
          "href": ${renkuApiUrl / "projects" / projectPath / "datasets" / name / "tags"}
        }]
      }"""
    case ProjectDataset(id, originalId, title, name, Right(derivedFrom), images) =>
      json"""{
        "identifier": $id,
        "versions" : {
          "initial": $originalId
        },
        "title":       $title,
        "name":        $name,
        "slug":        $name,
        "derivedFrom": $derivedFrom,
        "images":      ${images -> projectPath},
        "_links": [{
          "rel":  "details",
          "href": ${renkuApiUrl / "datasets" / id}
        }, {
          "rel":  "initial-version",
          "href": ${renkuApiUrl / "datasets" / originalId}
        }, {
          "rel":  "tags",
          "href": ${renkuApiUrl / "projects" / projectPath / "datasets" / name / "tags"}
        }]
      }"""
  }

  private lazy val projectPath = projectPaths.generateOne
  private implicit lazy val renkuApiUrl: renku.ApiUrl = renkuApiUrls.generateOne
  private implicit lazy val gitLabUrl:   GitLabUrl    = gitLabUrls.generateOne
}
