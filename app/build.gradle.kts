plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

val releaseVersionCode = 51
val releaseVersionName = "1.10.13"

val buildNativeAndroid by tasks.registering(org.gradle.api.tasks.Exec::class) {
    val script = rootProject.file("scripts/build-native-android.ps1")
    val nativeDir = rootProject.file("native/tgwsproxy")
    val goSources = files(
        rootProject.fileTree(nativeDir) { include("**/*.go", "go.mod", "go.sum") },
    )
    val output = project.file("src/main/jniLibs/arm64-v8a/libtgwsproxy.so")
    val shell = if (System.getProperty("os.name").lowercase().contains("windows")) "powershell" else "pwsh"

    inputs.file(script)
    inputs.files(goSources)
    outputs.file(output)
    workingDir = rootProject.projectDir
    commandLine(shell, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", script.absolutePath)
}

val generateAppIcons by tasks.registering(org.gradle.api.tasks.Exec::class) {
    val script = rootProject.file("scripts/generate-icons.py")
    val isWindows = System.getProperty("os.name").lowercase().contains("windows")
    inputs.file(script)
    inputs.file(rootProject.file("icon.png"))
    outputs.dir(project.file("src/main/res"))
    workingDir = rootProject.projectDir
    if (isWindows) {
        commandLine("py", "-3", script.absolutePath)
    } else {
        commandLine("python3", script.absolutePath)
    }
}

tasks.named("preBuild") {
    dependsOn(buildNativeAndroid, generateAppIcons)
}

android {
    namespace = "com.amurcanov.tgwsproxy"
    compileSdk = 35

    defaultConfig {
        applicationId = "com.amurcanov.tgwsproxy"
        minSdk = 26
        targetSdk = 35
        versionCode = releaseVersionCode
        versionName = releaseVersionName

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        vectorDrawables {
            useSupportLibrary = true
        }

        ndk {
            abiFilters.add("arm64-v8a")
        }
    }

    signingConfigs {
        val keystoreFile = rootProject.file(System.getenv("KEYSTORE_FILE") ?: "tgwsproxy-release.jks")
        val keystorePassword = System.getenv("KEYSTORE_PASSWORD")
        val keyPasswordValue = System.getenv("KEY_PASSWORD")
        val keyAliasValue = System.getenv("KEY_ALIAS") ?: "tgwsproxy"

        if (keystoreFile.exists() && !keystorePassword.isNullOrBlank() && !keyPasswordValue.isNullOrBlank()) {
            create("release") {
                storeFile = keystoreFile
                storePassword = keystorePassword
                keyAlias = keyAliasValue
                keyPassword = keyPasswordValue
            }
        }
    }

    buildTypes {
        debug {
            versionNameSuffix = "-debug"
        }
        release {
            signingConfig = signingConfigs.findByName("release")
            isMinifyEnabled = true
            isShrinkResources = true
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_1_8
        targetCompatibility = JavaVersion.VERSION_1_8
    }
    kotlinOptions {
        jvmTarget = "1.8"
    }
    testOptions {
        unitTests.isReturnDefaultValues = true
    }
    buildFeatures {
        compose = true
    }
    composeOptions {
        kotlinCompilerExtensionVersion = "1.5.8"
    }
    packaging {
        resources {
            excludes += "/META-INF/{AL2.0,LGPL2.1}"
        }
    }
    sourceSets {
        getByName("main") {
            jniLibs.srcDir("src/main/jniLibs")
        }
    }
}

androidComponents {
    onVariants(selector().withBuildType("debug")) { variant ->
        variant.outputs.forEach { output ->
            // Debug builds intentionally stay one code above the release code so they can be
            // installed over the most recent release during pre-release Android acceptance.
            output.versionCode.set(releaseVersionCode + 1)
        }
    }
}

dependencies {
    implementation("androidx.core:core-ktx:1.12.0")
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.7.0")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.7.0")
    implementation("androidx.activity:activity-compose:1.8.2")
    implementation(platform("androidx.compose:compose-bom:2024.01.00"))
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.ui:ui-graphics")
    implementation("androidx.compose.ui:ui-tooling-preview")
    implementation("androidx.compose.material3:material3")
    implementation("androidx.compose.material:material")
    implementation("net.java.dev.jna:jna:5.14.0@aar")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("androidx.compose.material:material-icons-extended")

    testImplementation("junit:junit:4.13.2")
}
