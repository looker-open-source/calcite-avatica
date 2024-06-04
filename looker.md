<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# Looker's branch of Calcite Avatica

This document describes how to develop Looker's branch of Calcite Avatica.

Do not merge to Avatica's main branch.

## Development

Looker has poor infrastructure for testing with local builds of Avatica.
The easiest way is to upload a snapshot version to Looker's Nexus repository and use it.
To upload a snapshot version, simply run `./looker-snapshot.sh`,
which runs `./gradlew build` and,
if successful, uploads the resulting artifacts to the repo
using the version number configured in `gradle.properties` (plus a "-SNAPSHOT" suffix).

Then, update the version in Looker's `internal/repositories/maven_deps.bzl` and repin,
and you're ready to build.

To build Calcite with a snapshot build of Avatica, use `./gradlew publishToMavenLocal`
to publish it in your `~/.m2/` local repository,
then enable the local repo in Calcite by un-commenting the line `enableMavenLocal=true`
in Calcite's `gradle.properties`, and pick the version of Avatica you just built.

## Building the Looker SDK

As of release `23.4-looker`, this project depends on the Looker SDK.
To build a new version of that SDK:

1. Spin up helltool on your cloudtop and make sure you have a user account
   with APIv3 credentials in there. Also make sure it has the self-signed
   certificates set up as per the helltool README.
1. Go to Admin > API and change the "API Host URL" from
   `http://localhost:19999` to `https://self-signed.looker.com:19999`
   and hit Save.
1. Clone the [SDK Codegen] repo and `cd` to the repository root.
1. `cp looker-sample.ini looker.ini`, and edit the values in `looker.ini`
   so it points to your local helltool server. You're going to hardcode
   credentials to your local instance, but these are only used to generate
   the Kotlin sources by querying your running helltool server
   for all available API endpoints. They won't end up in the built JAR.
   - `base_url` should be `https://self-signed.looker.com:19999`.
   - Set `client_id` and `client_secret` according to your APIv3 credentials.
   - Set `verify_ssl=false`.
1. `yarn install`
1. `yarn build`
1. `yarn gen kotlin`
1. `cd kotlin`
1. `./gradlew :spotlessApply`
1. `./gradlew build`.
   You may get some test failures. It's likely you could ignore these,
   although it probably warrants some investigation.
   The SDK codegen repo isn't always well-maintained.
   You'll end up with an SDK JAR at `build/libs/looker-kotlin-sdk.jar`.
1. Copy the SDK JAR over to the Avatica repo.
   Include the commit hash from the SDK repo when it was built in the filename.
   You should have a file like `libs/looker-kotlin-sdk-a48011f.jar` in the Calcite repo.
1. Edit `build.gradle.kts` to point to that file, like in the [initial commit]
   adding the SDK to the driver.

[SDK Codegen]: https://github.com/looker-open-source/sdk-codegen
[initial commit]: https://github.com/looker-open-source/calcite-avatica/commit/f989f12dc10af8215c6d1089aa090e96bc061f9b#diff-b6b51c3dfe033da6e3383076d95b8f5e692e53fbc74e6bc597407fbc49d2ba07

## Release

Release will have a name like `1.21.1-looker` (if the most
recent official Calcite release is `1.21`) and have a git tag
`avatica-1.21.1-looker`.

You should make it from a branch that differs from Avatica's
`main` branch in only minor ways:
* Cherry-pick commits from the previous `avatica-x.xx.x-looker`
  release that set up Looker's repositories (or, if you prefer,
  rebase the previous release branch onto the latest main)
* If necessary, include one or two commits for short-term fixes, but
  log [JIRA cases](https://issues.apache.org/jira/browse/CALCITE) to
  get them into `main` (use component `avatica`).
* Keep fixup commits coherent (squash incremental commits)
  and to a minimum.
* The final commit for a release must only set the version
  in `gradle.properties`, and it must be the only such commit
  that differs from Avatica trunk. Note that Avatica increments
  the version in trunk immediately after each release, so
  that version is typically unreleased. The Looker version
  should be named after the most recent Avatica release,
  so the version in trunk is generally decremented
  while adding the `-looker` suffix.

In Avatica's `gradle.properties`, update the value of
`calcite.avatica.version` to the release name (something like
`1.22.1-looker`) and commit.

Define Looker's Nexus repository in your `~/.gradle/init.gradle.kts`
file:

```kotlin
allprojects {
    plugins.withId("maven-publish") {
        configure<PublishingExtension> {
            repositories {
                maven {
                    name = "lookerNexus"
                    val baseUrl = "https://nexusrepo.looker.com"
                    val releasesUrl = "$baseUrl/repository/maven-releases"
                    val snapshotsUrl = "$baseUrl/repository/maven-snapshots"
                    val release = !project.version.toString().endsWith("-SNAPSHOT")
                    // val release = project.hasProperty("release")
                    url = uri(if (release) releasesUrl else snapshotsUrl)
                    credentials {
                        username = "xxx"
                        password = "xxx"
                    }
                }
            }
        }
    }
}
```

In the above fragment, replace the values of the `username` and
`password` properties with the secret credentials.

*NOTE* This fragment *must* be in a file outside of your git sandbox.
If the file were in the git sandbox, it would be too easy to
accidentally commit the secret credentials and expose them on a
public site.

Publish:
```sh
./gradlew -Prelease -PskipSign publishAllPublicationsToLookerNexusRepository
```

Check the artifacts
[on Nexus](https://nexusproxy.looker.com/#browse/search=keyword%3Dorg.apache.calcite.avatica).

If the release was successful, tag the release and push the tag:
```sh
git tag avatica-1.21.1-looker HEAD
git push looker avatica-1.21.1-looker
```
