package ch.datascience.graphservice.lineage

import java.io.Serializable

private object model {

  final case class Lineage(nodes: List[Node], edges: List[Edge])

  sealed trait Node extends Product with Serializable {
    val id:    String
    val label: String
  }
  object Node {
    final case class SourceNode(id: String, label: String) extends Node
    final case class TargetNode(id: String, label: String) extends Node
  }

  sealed trait Edge extends Product with Serializable {
    val id: String
  }
  object Edge {
    final case class SourceEdge(id: String) extends Edge
    final case class TargetEdge(id: String) extends Edge
  }
}
