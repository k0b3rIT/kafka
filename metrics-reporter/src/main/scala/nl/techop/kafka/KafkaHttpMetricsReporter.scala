/*
 * Copyright 2016, arnobroekhof@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.techop.kafka

import com.cloudera.kafka.prometheus.metrics.reporting.PrometheusMetricsServlet
import com.cloudera.kafka.wrap.Kafka
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider
import com.yammer.metrics.reporting._
import kafka.coordinator.group.GroupMetadataManager
import kafka.metrics.{KafkaMetricsConfig, KafkaMetricsReporterMBean, KafkaServerMetricsReporter}
import kafka.server.KafkaBroker
import kafka.utils.{Logging, VerifiableProperties}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.eclipse.jetty.security._
import org.eclipse.jetty.security.authentication.BasicAuthenticator
import org.eclipse.jetty.server.{HttpConfiguration, HttpConnectionFactory, Server, ServerConnector}
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.security.{Constraint, Password}
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.glassfish.jersey.server.ResourceConfig
import org.glassfish.jersey.servlet.ServletContainer

import java.util.regex.Pattern
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

private trait KafkaHttpMetricsReporterMBean extends KafkaMetricsReporterMBean

object KafkaHttpMetricsReporter {
  val defaultPort = 8080
  val defaultBindAddress = "localhost"
}

private class KafkaHttpMetricsReporter extends KafkaServerMetricsReporter
                              with KafkaHttpMetricsReporterMBean
                              with Logging {

  private var metricsServer: Server = null

  private var initialized = false
  private var configured = false
  private var running = false

  private var metricsConfig: KafkaMetricsConfig = null
  private var bindAddress: String = null
  private var port: Int = 0

  private var sslEnabledForMetrics = false
  private var kafkaConfig: VerifiableProperties = null

  private val COMMA_WITH_WHITESPACE = Pattern.compile("\\s*,\\s*")
  private val SslEnabledForMetrics = "kafka.http.metrics.ssl.enabled"
  private val BasicAuthEnabled = "kafka.http.metrics.authentication.enabled"
  private val BasicAuthUser = "kafka.http.metrics.user"

  override def getMBeanName = "kafka:type=nl.techop.kafka.KafkaHttpMetricsReporter"

  override def init(props: VerifiableProperties): Unit = {
    synchronized {
      if (!initialized) {
        info("Initializing Kafka Http Metrics Reporter")
        metricsConfig = new KafkaMetricsConfig(props)
        bindAddress = props.getString("kafka.http.metrics.host", KafkaHttpMetricsReporter.defaultBindAddress)
        port = props.getInt("kafka.http.metrics.port", KafkaHttpMetricsReporter.defaultPort)
        sslEnabledForMetrics = props.getBoolean(SslEnabledForMetrics, default = false)
        kafkaConfig = props

        initialized = true
        info("Initialized Kafka Http Metrics Reporter")
      } else {
        error("Kafka Http Metrics Reporter is already initialized")
      }
    }
  }

  override def setupAndStart(broker: Option[KafkaBroker]): Unit = {
    synchronized {
      if (initialized) {
        if (!configured) {
          info("Starting Kafka Http Metrics Reporter")

          // create new Jetty server
          metricsServer = new Server()

          val servletContextHandler: ServletContextHandler = new ServletContextHandler()
          if (kafkaConfig.getBoolean(BasicAuthEnabled, default = false)) {
            servletContextHandler.setSecurityHandler(getBasicAuthSecurityHandler(
              kafkaConfig.getString(BasicAuthUser, null),
              kafkaConfig.getString(Kafka.BasicAuthPassword, null),
              "kafka-metrics"))
          }
          servletContextHandler.setContextPath("/*")

          val httpConfig = new HttpConfiguration
          httpConfig.setSendServerVersion(false)

          if (sslEnabledForMetrics) {
            val sslContextFactory = new SslContextFactory.Server()
            setSSLKeysStoreConfigs(sslContextFactory)
            setTrustStoreConfigs(sslContextFactory)
            setFactoryAlgorithmConfigs(sslContextFactory)
            setAuthenticationConfigs(sslContextFactory)

            val httpConnector = new ServerConnector(metricsServer, sslContextFactory)
            httpConnector.setPort(port)
            httpConnector.setHost(bindAddress)
            metricsServer.setConnectors(Array(httpConnector))
          } else {
            val httpConnector = new ServerConnector(metricsServer, new HttpConnectionFactory(httpConfig))
            httpConnector.setPort(port)
            httpConnector.setHost(bindAddress)
            metricsServer.setConnectors(Array(httpConnector))
          }

          servletContextHandler.setAttribute(MetricsServlet.REGISTRY_ATTRIBUTE, KafkaYammerMetrics.defaultRegistry());

          // default (404 Servlet), thread, ping and healthcheck servlets
          addMetricsServlet(servletContextHandler, new DefaultServlet() with NoDoTrace, "/")
          addMetricsServlet(servletContextHandler, new ThreadDumpServlet() with NoDoTrace, "/api/threads")
          addMetricsServlet(servletContextHandler, new PingServlet() with NoDoTrace, "/api/ping")
          addMetricsServlet(servletContextHandler, new HealthCheckServlet() with NoDoTrace, "/api/healthcheck")

          // Add Metrics Servlets
          val customMetricsServlet = new CustomMetricsServlet(KafkaYammerMetrics.defaultRegistry()) with NoDoTrace
          KafkaYammerMetrics.defaultRegistry().addListener(customMetricsServlet)
          addMetricsServlet(servletContextHandler, customMetricsServlet, "/api/metrics")

          if (broker.isDefined) {
            brokerServlets(servletContextHandler, broker.get)
          }
          else {
            addPrometheusMetricsServlet(servletContextHandler)
          }

          // Add the handler to the server
          metricsServer.setHandler(servletContextHandler)

          configured = true
          startReporter(metricsConfig.pollingIntervalSecs)
          info("Started Kafka Http Metrics Reporter")
        } else {
          error("Kafka Http Metrics Reporter is already configured")
        }
      } else {
        error("Kafka Http Metrics Reporter is not initialized")
      }
    }
  }

  private def brokerServlets(servletContextHandler: ServletContextHandler, broker: KafkaBroker): Unit = {
    addPrometheusMetricsServlet(servletContextHandler, None)

    val resourceConfig: ResourceConfig = new ResourceConfig
    resourceConfig.register(new JacksonJsonProvider(), 0)
    resourceConfig.register(new KafkaTopicsResource(broker), 0)
    resourceConfig.register(new TopicMetricNameResource(broker), 0)

    val servletContainer: ServletContainer = new ServletContainer(resourceConfig)
    val servletHolder: ServletHolder = new ServletHolder(servletContainer)
    servletContextHandler.addServlet(servletHolder, "/api/*")
  }

  private def addPrometheusMetricsServlet(servletContextHandler: ServletContextHandler, groupManagerProvider: Option[() => GroupMetadataManager] = None): Unit = {
    val prometheusMetricsServlet = new PrometheusMetricsServlet(groupManagerProvider) with NoDoTrace
    KafkaYammerMetrics.defaultRegistry().addListener(prometheusMetricsServlet)
    addMetricsServlet(servletContextHandler, prometheusMetricsServlet, "/api/prometheus-metrics")
  }

  def getBasicAuthSecurityHandler(user: String, password: String, realm: String): SecurityHandler = {
    if (user == null || password == null) {
      throw new RuntimeException("Both username and password have to be provided if Basic Authentication is enabled!")
    }
    val hsl: HashLoginService = new HashLoginService()
    val userStore: UserStore = new UserStore()
    userStore.addUser(user, new Password(password), Array[String]("user"))
    hsl.setUserStore(userStore)

    val constraint: Constraint = new Constraint()
    constraint.setName(Constraint.__BASIC_AUTH)
    constraint.setRoles(Array[String]("user"))
    constraint.setAuthenticate(true)

    val cm: ConstraintMapping = new ConstraintMapping()
    cm.setConstraint(constraint)
    cm.setPathSpec("/*")

    val csh: ConstraintSecurityHandler = new ConstraintSecurityHandler()
    csh.setAuthenticator(new BasicAuthenticator())
    csh.setRealmName(realm)
    csh.addConstraintMapping(cm)
    csh.setLoginService(hsl)

    csh
  }

  private def setAuthenticationConfigs(sslContextFactory: SslContextFactory.Server): Unit = {
    sslContextFactory.setWantClientAuth(false)
    sslContextFactory.setNeedClientAuth(false)
  }

  private def setFactoryAlgorithmConfigs(sslContextFactory: SslContextFactory.Server): Unit = {
    val sslEnabledProtocolsTemp = kafkaConfig.getString(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, null)
    val sslProtocols: Array[String] = if (sslEnabledProtocolsTemp != null)
      COMMA_WITH_WHITESPACE.split(sslEnabledProtocolsTemp) else COMMA_WITH_WHITESPACE.split(SslConfigs.DEFAULT_SSL_ENABLED_PROTOCOLS)
    sslContextFactory.setIncludeProtocols(sslProtocols:_*)
    val cipherSuites = kafkaConfig.getString(SslConfigs.SSL_CIPHER_SUITES_CONFIG, null)
    if (cipherSuites != null) {
      sslContextFactory.setIncludeCipherSuites(COMMA_WITH_WHITESPACE.split(cipherSuites): _*)
    }
    val sslProvider = kafkaConfig.getString(SslConfigs.SSL_PROVIDER_CONFIG, null)
    if (sslProvider != null) {
      sslContextFactory.setProvider(sslProvider)
    }
    val sslProtocol = kafkaConfig.getString(SslConfigs.SSL_PROTOCOL_CONFIG, null)
    if (sslProtocol != null) {
      sslContextFactory.setProtocol(sslProtocol)
    }
    val sslKeyManagerAlgorithm = kafkaConfig.getString(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG, null)
    if (sslKeyManagerAlgorithm != null) {
      sslContextFactory.setKeyManagerFactoryAlgorithm(sslKeyManagerAlgorithm)
    }
    val trustManagerAlgorithm = kafkaConfig.getString(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG, null)
    if (trustManagerAlgorithm != null) {
      sslContextFactory.setTrustManagerFactoryAlgorithm(trustManagerAlgorithm)
    }
    val sslRandomImpl = kafkaConfig.getString(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG, null)
    if (sslRandomImpl != null) {
      sslContextFactory.setSecureRandomAlgorithm(sslRandomImpl)
    }
  }

  private def setSSLKeysStoreConfigs(sslContextFactory: SslContextFactory.Server): Unit = {
    val sslKeyStoreLocation = kafkaConfig.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, null)
    if (sslKeyStoreLocation != null) {
      sslContextFactory.setKeyStorePath(sslKeyStoreLocation)
    }
    val sslKeyStoreType = kafkaConfig.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, null)
    if (sslKeyStoreType != null) {
      sslContextFactory.setKeyStoreType(sslKeyStoreType)
    }
    val sslKeyStorePassword = kafkaConfig.getString(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, null)
    if (sslKeyStorePassword != null) {
      sslContextFactory.setKeyStorePassword(sslKeyStorePassword)
    }
    val sslKeyPassword = kafkaConfig.getString(SslConfigs.SSL_KEY_PASSWORD_CONFIG, null)
    if (sslKeyPassword != null) {
      sslContextFactory.setKeyManagerPassword(sslKeyPassword)
    }
  }

  private def setTrustStoreConfigs(sslContextFactory: SslContextFactory.Server): Unit = {
    val trustStorePath = kafkaConfig.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, null)
    if (trustStorePath != null) {
      sslContextFactory.setTrustStorePath(trustStorePath)
    }
    val trustStoreType = kafkaConfig.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, null)
    if (trustStoreType != null) {
      sslContextFactory.setTrustStoreType(trustStoreType)
    }
    val sslTrustStorePassword = kafkaConfig.getString(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, null)
    if (sslTrustStorePassword != null) {
      sslContextFactory.setTrustStorePassword(sslTrustStorePassword)
    }
  }

  private def addMetricsServlet(context: ServletContextHandler, servlet: HttpServlet, urlPattern: String): Unit = {
    context.addServlet(new ServletHolder(servlet), urlPattern)
  }

  private trait NoDoTrace extends HttpServlet {
    override def doTrace(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
      resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED)
    }
  }

  override def startReporter(pollingPeriodSecs: Long): Unit = {
    synchronized {
      if (initialized && configured) {
        if (!running) {
          metricsServer.start()
          running = true
          info(s"Started Kafka HTTP metrics reporter at ${metricsServer.getURI}")
        } else {
          error("Kafka Http Metrics Reporter is already running")
        }
      } else {
        error("Kafka Http Metrics Reporter is not initialized or not configured")
      }
    }
  }

  override def stopReporter(): Unit = {
    synchronized {
      if (initialized && configured) {
        if (running) {
          metricsServer.stop()
          running = false
          info("Stopped Kafka Http Metrics Reporter")
        } else {
          error("Kafka Http Metrics Reporter already stopped")
        }
      } else {
        error("Kafka Http Metrics Reporter is not initialized or not configured")
      }
    }
  }

}
