object JenaServer {

  def triplesStoreClient(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.triplesstore.client.util.TSClientJenaServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def projectAuth(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.projectauth.ProjectAuthJenaServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def commons(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.triplesstore.CommonsJenaServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def entitiesSearch(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.entities.EntitiesSearchJenaServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def viewingsCollector(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.entities.viewings.ViewingsCollectorJenaServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def triplesGenerator(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.triplesgenerator.TriplesGeneratorJenaServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def knowledgeGraph(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.knowledgegraph.KnowledgeGraphJenaServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def acceptanceTests(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.graph.acceptancetests.db.TriplesStore$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }
}
