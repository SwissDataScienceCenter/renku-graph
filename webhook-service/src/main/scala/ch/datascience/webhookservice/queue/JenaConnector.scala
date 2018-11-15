package ch.datascience.webhookservice.queue

import org.apache.jena.graph.Graph

import scala.concurrent.Future

private class JenaConnector {
  def persist(graph: Graph): Future[Unit] = ???

}
