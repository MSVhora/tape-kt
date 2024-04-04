tape-kt
====================

This is a kotlin version of Tape library provided by **Square, Inc.**. It also supports kotlin
coroutines and resolves file corruption and concurrent access issues.

Original Tape Library: [https://github.com/square/tape](https://github.com/square/tape)

Download
______

Kotlin DSL:

```
    dependencyResolutionManagement {
        repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
        repositories {
            google()
            mavenCentral()
            maven("https://jitpack.io")
        }
    }

    dependencies {
        implementation("com.github.MSVhora:tape-kt:1.0.4")
    }

```

Groovy DSL:

```
    dependencyResolutionManagement {
        repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
        repositories {
            mavenCentral()
            maven { url 'https://jitpack.io' }
        }
    }
            
    dependencies {
        implementation 'com.github.MSVhora:tape-kt:1.0.4'
    }
```

maven

```
	<repositories>
		<repository>
		    <id>jitpack.io</id>
		    <url>https://jitpack.io</url>
		</repository>
	</repositories>
	
	<dependency>
	    <groupId>com.github.MSVhora</groupId>
	    <artifactId>tape-kt</artifactId>
	    <version>1.0.4</version>
	</dependency>
```

License
-------

    Copyright 2024 Murtuza Vhora<murtazavhora@gmail.com>.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    Note: [Original tape library code is still copyrighted by Square, inc. ]
