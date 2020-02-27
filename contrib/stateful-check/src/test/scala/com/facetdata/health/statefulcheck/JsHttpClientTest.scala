package com.facetdata.health.statefulcheck

import com.facetdata.health.statefulcheck.StatefulCheckRunner.CheckResult
import com.metamx.common.scala.Predef._
import javax.servlet.Servlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.MustMatchers
import org.scalatest.junit.JUnitRunner
import org.scalatra.ScalatraServlet

@RunWith(classOf[JUnitRunner])
class JsHttpClientTest extends FunSuite with MustMatchers with BeforeAndAfterAll
{
  private lazy val server = initServletServer(
    servlets = Map(
      "/get/*" -> new ScalatraServlet
      {
        get("/") {
          "ok"
        }
      },
      "/post/*" -> new ScalatraServlet
      {
        post("/") {
          request.body
        }
      }
    )
  )

  test("HttpClient.get") {
    val script =
      """
        |try {
        | var response = HttpClient.get("http://localhost:9079/get", {});
        | Log.info(response);
        | CheckResult.create("echo", true, {"content":response.content(), "status":response.status(), "reason":response.reason()});
        |}
        |catch(e) {
        | Log.error(e.message);
        | CheckResult.create("echo", false, {"error":e.message});
        |}
      """.stripMargin
    val engine = JsScriptEngine.create()
    val response = engine.eval(script).asInstanceOf[CheckResult]
    response.ok mustBe true
    response.name mustBe "echo"
    response.data mustBe Map("content" -> "ok", "status" -> 200, "reason" -> "OK")
  }

  test("HttpClient.post") {
    val script =
      """
        |try {
        | var response = HttpClient.post("http://localhost:9079/post", {}, {"msg":"post"});
        | Log.info(response);
        | CheckResult.create("echo", true, {"content":response.content(), "status":response.status(), "reason":response.reason()});
        |}
        |catch(e) {
        | Log.error(e.message);
        | CheckResult.create("echo", false, {"error":e.message});
        |}
      """.stripMargin
    val engine = JsScriptEngine.create()
    val response = engine.eval(script).asInstanceOf[CheckResult]
    response.ok mustBe true
    response.name mustBe "echo"
    response.data mustBe Map("content" -> "{\"msg\":\"post\"}", "status" -> 200, "reason" -> "OK")
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    server.start()
  }


  override protected def afterAll(): Unit = {
    super.afterAll()
    server.stop()
  }

  def initServletServer(port: Int = 9079, maxThreads: Int = 6, servlets: Map[String, Servlet]): Server = {
    new Server(
      new QueuedThreadPool() withEffect { pool =>
        pool.setMinThreads(maxThreads)
        pool.setMaxThreads(maxThreads)
      }
    ) withEffect {
      srv =>
        srv.setConnectors(
          Array(
            new ServerConnector(srv) withEffect {
              _.setPort(port)
            }
          )
        )

        srv.setHandler(
          new ServletContextHandler(ServletContextHandler.SESSIONS) withEffect {
            ctxt => {
              ctxt.setContextPath("/")
              servlets foreach {
                case (path, servlet) =>
                  ctxt.addServlet(new ServletHolder(servlet), path)
              }
            }
          }
        )
    }
  }

}