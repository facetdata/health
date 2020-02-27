package com.metamx.health.mesos

import java.io.PrintWriter
import java.io.StringWriter
import java.net.URI
import java.net.URLClassLoader
import java.util
import javax.tools.JavaFileObject.Kind
import javax.tools.JavaCompiler
import javax.tools.SimpleJavaFileObject
import javax.tools.ToolProvider
import org.apache.curator.ensemble.exhibitor.Exhibitors.BackupConnectionStringProvider
import org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider
import org.apache.curator.ensemble.exhibitor.Exhibitors
import org.apache.curator.framework.CuratorFramework
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec
import org.scalatest.Outcome
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CuratorFrameworkBuilderProxyTest extends FlatSpec
{
  private val className = "CuratorFrameworkFakeImpl"

  private var temporaryFolder = new TemporaryFolder

  "CuratorFrameworkBuilder" should "provide proxy for implementation class of different class loader" in {
    val compiler: JavaCompiler = ToolProvider.getSystemJavaCompiler

    val writer = new StringWriter
    val out = new PrintWriter(writer)
    out.println(compileSource)
    out.close()
    val file: SimpleJavaFileObject = new SimpleJavaFileObject(new URI(s"string:///$className.java"), Kind.SOURCE)
    {
      override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = writer.toString
    }

    val folder = temporaryFolder.newFolder()
    val options = List("-d" , folder.getAbsolutePath)
    compiler.getTask(null, null, null, options.asJava, null, List(file).asJava).call

    val anotherLoader = new URLClassLoader(Array(folder.toURI.toURL)) // working directory
    val clazz = anotherLoader.loadClass(className)
    assert(clazz.getInterfaces.iterator.next eq classOf[CuratorFramework]) // parent class (interface) is the same

    val proxy = new CuratorFrameworkBuilder("localhost:9999")
      .createProxy(createFakeExhibitorProvider, clazz.newInstance.asInstanceOf[CuratorFramework])
    assert(proxy.getClass.getInterfaces.iterator.next eq classOf[CuratorFramework])
  }

  override protected def withFixture(test: NoArgTest): Outcome = {
    try {
      temporaryFolder.create()
      super.withFixture(test)
    } finally {
      temporaryFolder.delete()
    }
  }

  def createFakeExhibitorProvider: ExhibitorEnsembleProvider = {
    new ExhibitorEnsembleProvider(
      new Exhibitors(
        new util.ArrayList, 0, new BackupConnectionStringProvider()
        {
          override def getBackupConnectionString: String = ""
        }
      ), null, "", 0, null
    )
  }

  private val compileSource   =
    """
      import java.util.concurrent.TimeUnit;
      import org.apache.curator.CuratorZookeeperClient;
      import org.apache.curator.framework.CuratorFramework;
      import org.apache.curator.framework.api.*;
      import org.apache.curator.framework.api.UnhandledErrorListener;
      import org.apache.curator.framework.api.transaction.CuratorTransaction;
      import org.apache.curator.framework.imps.CuratorFrameworkState;
      import org.apache.curator.framework.listen.Listenable;
      import org.apache.curator.framework.state.ConnectionStateListener;
      import org.apache.curator.utils.EnsurePath;
      import org.apache.zookeeper.Watcher;

      public class %s implements CuratorFramework
      {
        @Override public void start() {}
        @Override public void close() {}
        @Override public CuratorFrameworkState getState() { return null; }
        @Override public boolean isStarted() { return false; }
        @Override public CreateBuilder create() { return null; }
        @Override public DeleteBuilder delete() { return null; }
        @Override public ExistsBuilder checkExists() { return null; }
        @Override public GetDataBuilder getData() { return null; }
        @Override public SetDataBuilder setData() { return null; }
        @Override public GetChildrenBuilder getChildren() { return null; }
        @Override public GetACLBuilder getACL() { return null; }
        @Override public SetACLBuilder setACL() { return null; }
        @Override public CuratorTransaction inTransaction() { return null; }
        @Override public void sync(String path, Object backgroundContextObject) { }
        @Override public void createContainers(String path) throws Exception { }
        @Override public SyncBuilder sync() { return null; }
        @Override public Listenable<ConnectionStateListener> getConnectionStateListenable() { return null; }
        @Override public Listenable<CuratorListener> getCuratorListenable() { return null; }
        @Override public Listenable<UnhandledErrorListener> getUnhandledErrorListenable() { return null; }
        @Override public CuratorFramework nonNamespaceView() { return null; }
        @Override public CuratorFramework usingNamespace(String newNamespace) { return null; }
        @Override public String getNamespace() { return null; }
        @Override public CuratorZookeeperClient getZookeeperClient() { return null; }
        @Override public EnsurePath newNamespaceAwareEnsurePath(String path) { return null; }
        @Override public void clearWatcherReferences(Watcher watcher) { }
        @Override public boolean blockUntilConnected(int maxWaitTime, TimeUnit units) throws InterruptedException { return false; }
        @Override public void blockUntilConnected() throws InterruptedException { }
      }
    """.format(className)
}
