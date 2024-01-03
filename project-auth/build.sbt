organization := "io.renku"
name := "project-auth"

Test / testOptions += Tests.Setup(jenaServer("start"))
Test / testOptions += Tests.Cleanup(jenaServer("forceStop"))

def jenaServer(methodName: String): ClassLoader => Unit = classLoader => {
  val clazz    = classLoader.loadClass("io.renku.projectauth.ProjectAuthJenaServer$")
  val method   = clazz.getMethod(methodName)
  val instance = clazz.getField("MODULE$").get(null)
  method.invoke(instance)
}

libraryDependencies ++= Dependencies.http4sClient
