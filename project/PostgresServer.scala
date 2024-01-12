// TODO start just one server for all the tests
object PostgresServer {

  def commons(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.db.CommonsPostgresServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def eventsQueue(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.eventsqueue.EventsQueuePostgresServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def eventLog(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.eventlog.EventLogPostgresServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def tokenRepository(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.tokenrepository.repository.TokenRepositoryPostgresServer$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }

  def acceptanceTests(methodName: String): ClassLoader => Unit = classLoader => {
    val clazz    = classLoader.loadClass("io.renku.graph.acceptancetests.db.PostgresDB$")
    val method   = clazz.getMethod(methodName)
    val instance = clazz.getField("MODULE$").get(null)
    method.invoke(instance)
  }
}
