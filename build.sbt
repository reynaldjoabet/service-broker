import Dependencies.*

ThisBuild / scalaVersion      := "3.3.8"
ThisBuild / version           := "0.1.0-SNAPSHOT"
ThisBuild / semanticdbEnabled := true

ThisBuild / scalacOptions := Seq(
  "-encoding",
  "UTF-8",
  "-no-indent",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-source:3.3",
  "-java-output-version:17",
  "-Werror",
  "-Wunused:all",
  "-Wvalue-discard",
  "-Wnonunit-statement",
  "-Xlint:all",
  "-Xcheck-macros",
  "-Xmax-inlines:64"
)

Global / onChangedBuildSource := ReloadOnSourceChanges

val generatedScalacOptions = Seq(
  "-encoding",
  "UTF-8",
  "-java-output-version:17",
  "-Xmax-inlines:64"
)

lazy val `open-service-broker-api-codegen` = (project in file("open-service-broker-api-codegen"))
  .enablePlugins(OpenApiGeneratorPlugin)
  .settings(
    name                 := "open-service-broker-api-codegen",
    scalacOptions        := generatedScalacOptions,
    openApiGeneratorName := "scala-sttp4-jsoniter",
    openApiInputSpec     := (baseDirectory.value / "spec.yaml").getPath,
    openApiConfigFile    := (baseDirectory.value / "config.json").getPath,
    // Kept at the module root rather than inside the output dir, so
    // src/main/scala holds nothing but generated Scala. Note the generator
    // resolves ignore rules relative to the directory holding the ignore file,
    // so every rule in it needs a leading **/ to reach src/main/scala.
    openApiIgnoreFileOverride      := (baseDirectory.value / ".openapi-generator-ignore").getPath,
    openApiOutputDir               := (baseDirectory.value / "src/main/scala").getAbsolutePath,
    openApiInvokerPackage          := "openservicebroker",
    openApiApiPackage              := "openservicebroker.api",
    openApiModelPackage            := "openservicebroker.model",
    openApiModelNamePrefix         := "",
    openApiModelNameSuffix         := "",
    openApiRemoveOperationIdPrefix := Some(true),
    openApiSkipOverwrite           := Some(false),
    openApiValidateSpec            := Some(true),
    // No .openapi-generator/FILES, VERSION or generated .openapi-generator-ignore:
    // the ignore file above is ours and checked in.
    openApiGenerateMetadata   := SettingDisabled,
    openApiGenerateModelTests := SettingDisabled,
    openApiGenerateApiTests   := SettingDisabled,

    generate := Def.uncached {
      openApiGenerate.value
    },

    Compile / sourceGenerators += generate.taskValue,

    Compile / unmanagedSourceDirectories := Seq.empty,
    libraryDependencies                 ++= Seq(
      sttpJsoniter,
      jsoniter,
      jsoniterMacros,
      jsoniterCirce
    )
  )

lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name                 := "service-broker",
    libraryDependencies ++= Seq(
      catsEffect,
      emberServer,
      http4sDsl,
      tapirCore,
      tapirHttp4sServer,
      tapirJsoniterScala,
      jsoniter,
      jsoniterMacros,
      jsoniterCirce,
      circeCore,
      circeParser,
      skunkCore,
      flyway,
      postgres,
      pureconfig,
      pureconfigGeneric,
      scribe,
      scribeSlf4j,
      munit,
      munitCatsEffect
    ),
    buildInfoKeys    := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "servicebroker"
  )
  .dependsOn(`open-service-broker-api-codegen` % "compile->compile")
