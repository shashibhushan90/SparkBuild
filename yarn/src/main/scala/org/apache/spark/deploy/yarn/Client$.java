object Client extends Logging {
  def main(argStrings: Array[String]) {
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a " +
        "future version of Spark. Use ./bin/spark-submit with \"--master yarn\"")
    }

    // Set an env variable indicating we are running in YARN mode.
    // Note that any env variable with the SPARK_ prefix gets propagated to all (remote) processes
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf

    val args = new ClientArguments(argStrings, sparkConf)
    new Client(args, sparkConf).run()
  }

  // Alias for the Spark assembly jar and the user jar
  val SPARK_JAR: String = "__spark__.jar"
  val APP_JAR: String = "__app__.jar"

  // URI scheme that identifies local resources
  val LOCAL_SCHEME = "local"

  // Staging directory for any temporary jars or files
  val SPARK_STAGING: String = ".sparkStaging"

  // Location of any user-defined Spark jars
  val CONF_SPARK_JAR = "spark.yarn.jar"
  val ENV_SPARK_JAR = "SPARK_JAR"

  // Internal config to propagate the location of the user's jar to the driver/executors
  val CONF_SPARK_USER_JAR = "spark.yarn.user.jar"

  // Internal config to propagate the locations of any extra jars to add to the classpath
  // of the executors
  val CONF_SPARK_YARN_SECONDARY_JARS = "spark.yarn.secondary.jars"

  // Staging directory is private! -> rwx--------
  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)

  // App files are world-wide readable and owner writable -> rw-r--r--
  val APP_FILE_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

  // Distribution-defined classpath to add to processes
  val ENV_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH"

  // Subdirectory where the user's hadoop config files will be placed.
  val LOCALIZED_HADOOP_CONF_DIR = "__hadoop_conf__"

  /**
   * Find the user-defined Spark jar if configured, or return the jar containing this
   * class if not.
   *
   * This method first looks in the SparkConf object for the CONF_SPARK_JAR key, and in the
   * user environment if that is not found (for backwards compatibility).
   */
  private def sparkJar(conf: SparkConf): String = {
    if (conf.contains(CONF_SPARK_JAR)) {
      conf.get(CONF_SPARK_JAR)
    } else if (System.getenv(ENV_SPARK_JAR) != null) {
      logWarning(
        s"$ENV_SPARK_JAR detected in the system environment. This variable has been deprecated " +
          s"in favor of the $CONF_SPARK_JAR configuration variable.")
      System.getenv(ENV_SPARK_JAR)
    } else {
      SparkContext.jarOfClass(this.getClass).head
    }
  }

  /**
   * Return the path to the given application's staging directory.
   */
  private def getAppStagingDir(appId: ApplicationId): String = {
    buildPath(SPARK_STAGING, appId.toString())
  }

  /**
   * Populate the classpath entry in the given environment map with any application
   * classpath specified through the Hadoop and Yarn configurations.
   */
  private[yarn] def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String])
    : Unit = {
    val classPathElementsToAdd = getYarnAppClasspath(conf) ++ getMRAppClasspath(conf)
    for (c <- classPathElementsToAdd.flatten) {
      YarnSparkHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }

  private def getYarnAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultYarnApplicationClasspath
    }

  private def getMRAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings("mapreduce.application.classpath")) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultMRApplicationClasspath
    }

  private[yarn] def getDefaultYarnApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[YarnConfiguration].getField("DEFAULT_YARN_APPLICATION_CLASSPATH")
      val value = field.get(null).asInstanceOf[Array[String]]
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default YARN Application classpath.", f.exception)
      case s: Success[Seq[String]] =>
        logDebug(s"Using the default YARN application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  /**
   * In Hadoop 0.23, the MR application classpath comes with the YARN application
   * classpath. In Hadoop 2.0, it's an array of Strings, and in 2.2+ it's a String.
   * So we need to use reflection to retrieve it.
   */
  private[yarn] def getDefaultMRApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[MRJobConfig].getField("DEFAULT_MAPREDUCE_APPLICATION_CLASSPATH")
      val value = if (field.getType == classOf[String]) {
        StringUtils.getStrings(field.get(null).asInstanceOf[String]).toArray
      } else {
        field.get(null).asInstanceOf[Array[String]]
      }
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default MR Application classpath.", f.exception)
      case s: Success[Seq[String]] =>
        logDebug(s"Using the default MR application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }

  /**
   * Populate the classpath entry in the given environment map.
   *
   * User jars are generally not added to the JVM's system classpath; those are handled by the AM
   * and executor backend. When the deprecated `spark.yarn.user.classpath.first` is used, user jars
   * are included in the system classpath, though. The extra class path and other uploaded files are
   * always made available through the system class path.
   *
   * @param args Client arguments (when starting the AM) or null (when starting executors).
   */
  private[yarn] def populateClasspath(
      args: ClientArguments,
      conf: Configuration,
      sparkConf: SparkConf,
      env: HashMap[String, String],
      isAM: Boolean,
      extraClassPath: Option[String] = None): Unit = {
    extraClassPath.foreach(addClasspathEntry(_, env))
    addClasspathEntry(
      YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), env
    )

    if (isAM) {
      addClasspathEntry(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD) + Path.SEPARATOR +
          LOCALIZED_HADOOP_CONF_DIR, env)
    }

    if (sparkConf.getBoolean("spark.yarn.user.classpath.first", false)) {
      val userClassPath =
        if (args != null) {
          getUserClasspath(Option(args.userJar), Option(args.addJars))
        } else {
          getUserClasspath(sparkConf)
        }
      userClassPath.foreach { x =>
        addFileToClasspath(x, null, env)
      }
    }
    addFileToClasspath(new URI(sparkJar(sparkConf)), SPARK_JAR, env)
    populateHadoopClasspath(conf, env)
    sys.env.get(ENV_DIST_CLASSPATH).foreach(addClasspathEntry(_, env))
  }

  /**
   * Returns a list of URIs representing the user classpath.
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: SparkConf): Array[URI] = {
    getUserClasspath(conf.getOption(CONF_SPARK_USER_JAR),
      conf.getOption(CONF_SPARK_YARN_SECONDARY_JARS))
  }

  private def getUserClasspath(
      mainJar: Option[String],
      secondaryJars: Option[String]): Array[URI] = {
    val mainUri = mainJar.orElse(Some(APP_JAR)).map(new URI(_))
    val secondaryUris = secondaryJars.map(_.split(",")).toSeq.flatten.map(new URI(_))
    (mainUri ++ secondaryUris).toArray
  }

  /**
   * Adds the given path to the classpath, handling "local:" URIs correctly.
   *
   * If an alternate name for the file is given, and it's not a "local:" file, the alternate
   * name will be added to the classpath (relative to the job's work directory).
   *
   * If not a "local:" file and no alternate name, the environment is not modified.
   *
   * @param uri       URI to add to classpath (optional).
   * @param fileName  Alternate name for the file (optional).
   * @param env       Map holding the environment variables.
   */
  private def addFileToClasspath(
      uri: URI,
      fileName: String,
      env: HashMap[String, String]): Unit = {
    if (uri != null && uri.getScheme == LOCAL_SCHEME) {
      addClasspathEntry(uri.getPath, env)
    } else if (fileName != null) {
      addClasspathEntry(buildPath(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD), fileName), env)
    }
  }

  /**
   * Add the given path to the classpath entry of the given environment map.
   * If the classpath is already set, this appends the new path to the existing classpath.
   */
  private def addClasspathEntry(path: String, env: HashMap[String, String]): Unit =
    YarnSparkHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, path)

  /**
   * Obtains token for the Hive metastore and adds them to the credentials.
   */
  private def obtainTokenForHiveMetastore(conf: Configuration, credentials: Credentials) {
    if (UserGroupInformation.isSecurityEnabled) {
      val mirror = universe.runtimeMirror(getClass.getClassLoader)

      try {
        val hiveClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.ql.metadata.Hive")
        val hive = hiveClass.getMethod("get").invoke(null)

        val hiveConf = hiveClass.getMethod("getConf").invoke(hive)
        val hiveConfClass = mirror.classLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf")

        val hiveConfGet = (param: String) => Option(hiveConfClass
          .getMethod("get", classOf[java.lang.String])
          .invoke(hiveConf, param))

        val metastore_uri = hiveConfGet("hive.metastore.uris")

        // Check for local metastore
        if (metastore_uri != None && metastore_uri.get.toString.size > 0) {
          val metastore_kerberos_principal_conf_var = mirror.classLoader
            .loadClass("org.apache.hadoop.hive.conf.HiveConf$ConfVars")
            .getField("METASTORE_KERBEROS_PRINCIPAL").get("varname").toString

          val principal = hiveConfGet(metastore_kerberos_principal_conf_var)

          val username = Option(UserGroupInformation.getCurrentUser().getUserName)
          if (principal != None && username != None) {
            val tokenStr = hiveClass.getMethod("getDelegationToken",
              classOf[java.lang.String], classOf[java.lang.String])
              .invoke(hive, username.get, principal.get).asInstanceOf[java.lang.String]

            val hive2Token = new Token[DelegationTokenIdentifier]()
            hive2Token.decodeFromUrlString(tokenStr)
            credentials.addToken(new Text("hive.server2.delegation.token"), hive2Token)
            logDebug("Added hive.Server2.delegation.token to conf.")
            hiveClass.getMethod("closeCurrent").invoke(null)
          } else {
            logError("Username or principal == NULL")
            logError(s"""username=${username.getOrElse("(NULL)")}""")
            logError(s"""principal=${principal.getOrElse("(NULL)")}""")
            throw new IllegalArgumentException("username and/or principal is equal to null!")
          }
        } else {
          logDebug("HiveMetaStore configured in localmode")
        }
      } catch {
        case e: java.lang.NoSuchMethodException => { logInfo("Hive Method not found " + e); return }
        case e: java.lang.ClassNotFoundException => { logInfo("Hive Class not found " + e); return }
        case e: Exception => { logError("Unexpected Exception " + e)
          throw new RuntimeException("Unexpected exception", e)
        }
      }
    }
  }

  /**
   * Obtain security token for HBase.
   */
  def obtainTokenForHBase(conf: Configuration, credentials: Credentials): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      val mirror = universe.runtimeMirror(getClass.getClassLoader)

      try {
        val confCreate = mirror.classLoader.
          loadClass("org.apache.hadoop.hbase.HBaseConfiguration").
          getMethod("create", classOf[Configuration])
        val obtainToken = mirror.classLoader.
          loadClass("org.apache.hadoop.hbase.security.token.TokenUtil").
          getMethod("obtainToken", classOf[Configuration])

        logDebug("Attempting to fetch HBase security token.")

        val hbaseConf = confCreate.invoke(null, conf)
        val token = obtainToken.invoke(null, hbaseConf).asInstanceOf[Token[TokenIdentifier]]
        credentials.addToken(token.getService, token)

        logInfo("Added HBase security token to credentials.")
      } catch {
        case e: java.lang.NoSuchMethodException =>
          logInfo("HBase Method not found: " + e)
        case e: java.lang.ClassNotFoundException =>
          logDebug("HBase Class not found: " + e)
        case e: java.lang.NoClassDefFoundError =>
          logDebug("HBase Class not found: " + e)
        case e: Exception =>
          logError("Exception when obtaining HBase security token: " + e)
      }
    }
  }

  /**
   * Return whether the two file systems are the same.
   */
  private def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
    val srcUri = srcFs.getUri()
    val dstUri = destFs.getUri()
    if (srcUri.getScheme() == null || srcUri.getScheme() != dstUri.getScheme()) {
      return false
    }

    var srcHost = srcUri.getHost()
    var dstHost = dstUri.getHost()

    // In HA or when using viewfs, the host part of the URI may not actually be a host, but the
    // name of the HDFS namespace. Those names won't resolve, so avoid even trying if they
    // match.
    if (srcHost != null && dstHost != null && srcHost != dstHost) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName()
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName()
      } catch {
        case e: UnknownHostException =>
          return false
      }
    }

    Objects.equal(srcHost, dstHost) && srcUri.getPort() == dstUri.getPort()
  }

  /**
   * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
   * This is used for preparing local resources to be included in the container launch context.
   */
  private def getQualifiedLocalPath(localURI: URI, hadoopConf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }

  /**
   * Whether to consider jars provided by the user to have precedence over the Spark jars when
   * loading user classes.
   */
  def isUserClassPathFirst(conf: SparkConf, isDriver: Boolean): Boolean = {
    if (isDriver) {
      conf.getBoolean("spark.driver.userClassPathFirst", false)
    } else {
      conf.getBoolean("spark.executor.userClassPathFirst", false)
    }
  }

  /**
   * Joins all the path components using Path.SEPARATOR.
   */
  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

}
