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

package ch.datascience.knowledgegraph.lineage

import LineageGenerators._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.knowledgegraph.lineage.model.Node.{SourceNode, TargetNode}
import ch.datascience.knowledgegraph.lineage.model.{NodeId, NodeLabel}
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class NodeSpec extends WordSpec with ScalaCheckPropertyChecks {

  "equals" should {

    "return true for Source and Target nodes having the same id and label" in {
      forAll { (nodeId: NodeId, nodeLabel: NodeLabel) =>
        SourceNode(nodeId, nodeLabel) shouldBe TargetNode(nodeId, nodeLabel)
      }
    }

    "return false if compared to a Node with a different id" in {
      forAll { (nodeId1: NodeId, nodeLabel: NodeLabel) =>
        val nodeId2 = nodeIds generateDifferentThan nodeId1
        SourceNode(nodeId1, nodeLabel) should not be SourceNode(nodeId2, nodeLabel)
        TargetNode(nodeId1, nodeLabel) should not be TargetNode(nodeId2, nodeLabel)
      }
    }

    "return false if compared to a Node with a different label" in {
      forAll { (nodeId: NodeId, nodeLabel1: NodeLabel) =>
        val nodeLabel2 = nodeLabels generateDifferentThan nodeLabel1
        SourceNode(nodeId, nodeLabel1) should not be SourceNode(nodeId, nodeLabel2)
        TargetNode(nodeId, nodeLabel1) should not be TargetNode(nodeId, nodeLabel2)
      }
    }

    "return false if compared to non-Node object" in {
      (sourceNodes.generateOne equals nonEmptyStrings().generateOne) shouldBe false
    }
  }
}
