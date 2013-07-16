package spark.metrics

import java.util.Properties
import java.io.{File, FileInputStream}

import scala.collection.mutable
import scala.util.matching.Regex

private[spark] class MetricsConfig(val configFile: Option[String]) {
  val properties = new Properties()
  val DEFAULT_PREFIX = "*"
  val INSTANCE_REGEX = "^(\\*|[a-zA-Z]+)\\.(.+)".r
  var propertyCategories: mutable.HashMap[String, Properties] = null

  private def setDefaultProperties(prop: Properties) {
    prop.setProperty("*.sink.jmx.class", "spark.metrics.sink.JmxSink")
  }

  def initilize() {
    //Add default properties in case there's no properties file
    setDefaultProperties(properties)

    configFile map { f =>
      val confFile = new File(f)
      if (confFile.exists()) {
        var fis: FileInputStream = null
        try {
          fis = new FileInputStream(confFile)
          properties.load(fis)
        } finally {
          fis.close()
        }
      }
    }

    propertyCategories = subProperties(properties, INSTANCE_REGEX)
    if (propertyCategories.contains(DEFAULT_PREFIX)) {
      import scala.collection.JavaConversions._

      val defaultProperty = propertyCategories(DEFAULT_PREFIX)
      for { (inst, prop) <- propertyCategories
            if (inst != DEFAULT_PREFIX)
            (k, v) <- defaultProperty
            if (prop.getProperty(k) == null) } {
        prop.setProperty(k, v)
      }
    }
  }

  def subProperties(prop: Properties, regex: Regex): mutable.HashMap[String, Properties] = {
    val subProperties = new mutable.HashMap[String, Properties]
    import scala.collection.JavaConversions._
    prop.foreach { kv =>
      if (regex.findPrefixOf(kv._1) != None) {
        val regex(prefix, suffix) = kv._1
        subProperties.getOrElseUpdate(prefix, new Properties).setProperty(suffix, kv._2)
      }
    }
    subProperties
  }

  def getInstance(inst: String): Properties = {
    propertyCategories.get(inst) match {
      case Some(s) => s
      case None => propertyCategories(DEFAULT_PREFIX)
    }
  }
}

