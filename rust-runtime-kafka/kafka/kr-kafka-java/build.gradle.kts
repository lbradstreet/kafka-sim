plugins { `java-library` }

group = "io.krkafka"
version = "0.1.0"

repositories { mavenCentral() }
java {
    modularity.inferModulePath = false
    toolchain { languageVersion = JavaLanguageVersion.of(25) }
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api("org.apache.kafka:kafka-clients:4.3.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.13.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.13.4")
}

dependencyLocking { lockAllConfigurations() }
tasks.withType<JavaCompile>().configureEach {
    options.release = 25
    options.encoding = "UTF-8"
}
// Kafka 4.3.0 has a filename-derived automatic module, which Gradle's
// inference intentionally excludes. Compile the real, unmodified jar on the
// explicit module path; tests exercise classpath and module-path launches.
tasks.compileJava {
    doFirst {
        options.compilerArgs.addAll(listOf("--module-path", classpath.asPath))
        classpath = files()
    }
}
tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    systemProperty("kr.kafka.test.classpath", sourceSets.test.get().runtimeClasspath.asPath)
    systemProperty("kr.kafka.test.modulepath", sourceSets.main.get().output.classesDirs.asPath
        + File.pathSeparator + configurations.compileClasspath.get().asPath)
    val defaultNativeLibrary = rootDir.resolve("../../target/debug/" + System.mapLibraryName("kr_kafka_ffi")).canonicalPath
    val nativeLibrary = file(providers.systemProperty("kr.kafka.library").orElse(defaultNativeLibrary).get()).canonicalFile
    val nativeTestLibrary = file(providers.systemProperty("kr.kafka.test.library").orElse(defaultNativeLibrary).get()).canonicalFile
    // A stable pathname does not imply stable native code. Fingerprint both
    // downcall libraries so replacing either one invalidates the test results.
    inputs.file(nativeLibrary).withPropertyName("nativeLibrary").withPathSensitivity(PathSensitivity.ABSOLUTE)
    inputs.file(nativeTestLibrary).withPropertyName("nativeTestLibrary").withPathSensitivity(PathSensitivity.ABSOLUTE)
    systemProperty("kr.kafka.library", nativeLibrary.path)
    systemProperty("kr.kafka.test.library", nativeTestLibrary.path)
    testLogging { events("failed", "skipped"); showStandardStreams = true }
}
tasks.jar { manifest.attributes["Enable-Native-Access"] = "ALL-UNNAMED" }
tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}
tasks.javadoc {
    (options as StandardJavadocDocletOptions).addBooleanOption("Xdoclint:none", true)
    doFirst {
        (options as StandardJavadocDocletOptions).modulePath = configurations.compileClasspath.get().files.toList()
        classpath = files()
    }
}

// Ordinary class jars remain useful for explicit-library deployments. The
// release artifact includes both verified Linux libraries and extraction hashes.
val nativeResources = layout.buildDirectory.dir("generated/nativeResources")
val verifyNativeResources = tasks.register<Exec>("verifyNativeResources") {
    group = "verification"
    description = "Verify native ABI, architecture, glibc floor, checksums and compiler-input freshness."
    commandLine("python3", rootDir.resolve("../../scripts/kafka-java-native.py"),
        "--verify-only", "--output", nativeResources.get().asFile)
}
val releaseJar = tasks.register<Jar>("releaseJar") {
    group = "build"
    description = "Build the distributable jar containing both verified Linux production libraries."
    dependsOn(tasks.classes, verifyNativeResources)
    archiveClassifier = "linux"
    from(sourceSets.main.get().output)
    from(nativeResources)
    manifest.attributes["Enable-Native-Access"] = "ALL-UNNAMED"
}
