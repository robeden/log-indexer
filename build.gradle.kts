import java.nio.charset.StandardCharsets

plugins {
    `java-library`
    `maven-publish`
    signing
    id("org.jreleaser") version "1.20.0"
}

group = "com.logicartisan"


version = project.property("VERSION") as String


dependencies {
    api("org.slf4j:slf4j-api:1.7.21")
    api("com.google.code.findbugs:jsr305:3.0.1")
    api("net.java.dev.glazedlists:glazedlists_java15:1.9.1")

    implementation("net.sf.trove4j:trove4j:3.0.3")
    implementation("com.logicartisan:common-core:1.1.0")

    testImplementation("org.easymock:easymock:5.2.0")
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}


repositories {
    mavenCentral()
}

java {
    withJavadocJar()
    withSourcesJar()
}

val testJavaVersion = System.getProperty("test.java.version", "21").toInt()
tasks.named<Test>("test") {
    useJUnitPlatform()
    testLogging {
        showStandardStreams = true
    }

    val javaToolchains = project.extensions.getByType<JavaToolchainService>()
    javaLauncher.set(javaToolchains.launcherFor {
        languageVersion.set(JavaLanguageVersion.of(testJavaVersion))
    })
}

tasks.withType<JavaCompile> {
    options.encoding = StandardCharsets.UTF_8.toString()
    sourceCompatibility = JavaVersion.VERSION_11.toString()
    targetCompatibility = JavaVersion.VERSION_11.toString()
}


tasks.named<Javadoc>("javadoc") {
    // Disable warnings when methods aren't commented.
    // See https://github.com/gradle/gradle/issues/15209 for why this crazy cast is happening.
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:missing", "-quiet")
}

tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = project.name
            from(components["java"])
            pom {
                name = project.name
                description = "Dynamic indexing and access to logs or other continually updating files"
                url = "https://github.com/robeden/log-indexer/"
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                developers {
                    developer {
                        id = "robeden"
                        name = "Rob Eden"
                        email = "rob@robeden.com"
                    }
                }
                scm {
                    url = "https://github.com/robeden/log-indexer/"
                    connection = "scm:git:git://github.com/robeden/log-indexer.git"
                    developerConnection = "scm:git:ssh://git@github.com/robeden/log-indexer.git"
                }
            }
        }
    }
    repositories {
        maven {
            name = "PreDeploy"
            url = uri(layout.buildDirectory.dir("pre-deploy"))
        }
    }
}

signing {
    val signingInMemoryKey: String? by project
    val signingPassword: String? by project
    useInMemoryPgpKeys(signingInMemoryKey, signingPassword)
    sign(publishing.publications["mavenJava"])
}


jreleaser {
    project {
        copyright = "Rob Eden"
        description = "Dynamic indexing and access to logs or other continually updating files"
    }
    signing {
        setActive("ALWAYS")
        setMode("MEMORY")
        armored = true
    }
    deploy {
        maven {
            mavenCentral {
                create("sonatype") {
                    setActive("ALWAYS")
                    url = "https://central.sonatype.com/api/v1/publisher"
                    stagingRepository("build/pre-deploy")
                    username = findProperty("ossrhUsername")?.toString() ?: System.getenv("OSSRH_USERNAME")
                    password = findProperty("ossrhPassword")?.toString() ?: System.getenv("OSSRH_PASSWORD")
                }
            }
        }
    }
    release {
        github {
            enabled = false
        }
    }
}