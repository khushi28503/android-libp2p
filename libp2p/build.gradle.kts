plugins {
    id("com.android.library")
    id("org.jetbrains.kotlin.android")
}

// Automatically reassemble golib.aar from split parts if needed
val reassembleGolib = tasks.register("reassembleGolib") {
    val targetAar = file("libs/golib.aar")
    val parts = (0..10).map { i -> file("libs/golib.aar.p${String.format("%02d", i)}") }

    inputs.files(parts.filter { it.exists() })
    outputs.file(targetAar)

    doLast {
        if (!targetAar.exists() && parts.all { it.exists() }) {
            targetAar.outputStream().use { out ->
                parts.forEach { part ->
                    part.inputStream().use { it.copyTo(out) }
                }
            }
            logger.lifecycle("Successfully reassembled golib.aar from ${parts.size} parts.")
        }
    }
}

tasks.named("preBuild") {
    dependsOn(reassembleGolib)
}

android {
    namespace = "io.libp2p.android"
    compileSdk = 35

    defaultConfig {
        minSdk = 24
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
        consumerProguardFiles("consumer-rules.pro")
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }
}

dependencies {
    // Go mobile bridge (precompiled libp2p binary)
    implementation(files("libs/golib.aar"))

    // JSON parsing
    implementation("com.google.code.gson:gson:2.11.0")

    // Kotlin coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.9.0")

    // Unit testing
    testImplementation("junit:junit:4.13.2")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
    testImplementation("com.google.code.gson:gson:2.11.0")
}
