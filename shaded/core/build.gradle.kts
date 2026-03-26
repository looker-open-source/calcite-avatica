/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.github.vlsi.gradle.crlf.CrLfSpec
import com.github.vlsi.gradle.crlf.LineEndings
import com.github.vlsi.gradle.license.GatherLicenseTask
import com.github.vlsi.gradle.license.api.SpdxLicense
import com.github.vlsi.gradle.release.Apache2LicenseRenderer
import com.github.vlsi.gradle.release.ArtifactType
import com.github.vlsi.gradle.release.dsl.dependencyLicenses
import com.github.vlsi.gradle.release.dsl.licensesCopySpec
import com.github.vlsi.gradle.license.api.SimpleLicense
import com.github.vlsi.gradle.license.api.SpdxLicenseException
import com.github.vlsi.gradle.license.api.and
import com.github.vlsi.gradle.license.api.with

plugins {
    `java-library`
    id("com.github.johnrengelman.shadow")
    id("com.github.vlsi.crlf")
    id("com.github.vlsi.license-gather")
    id("com.github.vlsi.stage-vote-release")
}

val shaded by configurations.creating

configurations {
    compileOnly {
        extendsFrom(shaded)
    }
}

dependencies {
    shaded(project(":core"))
    testImplementation("junit:junit")
}

tasks {
    val getLicenses by registering(GatherLicenseTask::class) {
        configuration(shaded)
        extraLicenseDir.set(file("$rootDir/src/main/config/licenses"))

        expectLicense("com.google.protobuf:protobuf-java", SpdxLicense.BSD_3_Clause)

        expectLicense("com.google.auth:google-auth-library-oauth2-http:1.24.0", SpdxLicense.BSD_3_Clause)
        expectLicense("com.google.api:api-common:2.30.0", SpdxLicense.BSD_3_Clause)
        expectLicense("com.google.api:gax:2.47.0", SpdxLicense.BSD_3_Clause)
        expectLicense("com.google.auth:google-auth-library-credentials:1.24.0", SpdxLicense.BSD_3_Clause)
        expectLicense("com.google.api:gax-grpc:2.47.0", SpdxLicense.BSD_3_Clause)
        expectLicense("com.google.api:gax-httpjson:2.47.0", SpdxLicense.BSD_3_Clause)
        expectLicense("com.google.protobuf:protobuf-java-util:3.25.3", SpdxLicense.BSD_3_Clause)

        expectLicense("org.codehaus.mojo:animal-sniffer-annotations:1.23", SpdxLicense.MIT)

        overrideLicense("com.google.re2j:re2j:1.7") {
            expectedLicense = SimpleLicense("Go License", uri("https://golang.org/LICENSE"))
            effectiveLicense = SpdxLicense.BSD_3_Clause
        }

        overrideLicense("javax.annotation:javax.annotation-api:1.3.2") {
            expectedLicense = SimpleLicense(
                "CDDL + GPLv2 with classpath exception",
                uri("https://github.com/javaee/javax.annotation/blob/master/LICENSE")
            )
            effectiveLicense = SpdxLicense.CDDL_1_1 and (SpdxLicense.GPL_2_0_or_later with SpdxLicenseException.Classpath_exception_2_0)
        }
    }

    val license by registering(Apache2LicenseRenderer::class) {
        group = LifecycleBasePlugin.BUILD_GROUP
        description = "Generates LICENSE file for the uberjar"
        artifactType.set(ArtifactType.BINARY)
        metadata.from(getLicenses)
    }

    val licenseFiles = licensesCopySpec(license)

    shadowJar {
        archiveClassifier.set("shadow")
        configurations = listOf(shaded)
        exclude("META-INF/maven/**")
        exclude("META-INF/LICENSE*")
        exclude("META-INF/NOTICE*")
        listOf(
            "com.fasterxml.jackson",
            "com.google.protobuf",
            "org.apache.hc",
            "org.apache.commons"
        ).forEach {
            relocate(it, "${project.group}.shaded.$it")
        }
        CrLfSpec(LineEndings.LF).run {
            into("META-INF") {
                textAuto()
                dependencyLicenses(licenseFiles)
            }
        }
    }

    jar {
        enabled = false
        dependsOn(shadowJar)
    }

    test {
        dependsOn(shadowJar)
    }
}

artifacts {
    extraMavenPublications(tasks.shadowJar) {
        classifier = ""
    }
}
