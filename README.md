# service-broker
A scala library that provides the REST API implementation for the OSB API.
The Service Catalog project is responsible for integrating Service Brokers to the Kubernetes ecosystem.
## Other Examples
- Cloud service broker: This service broker uses Terraform to provision and bind services. 
- Asynchronous Service Broker for AWS EC2: This Service Broker implements support for the Asynchronous Service Operations, and calls AWS APIs to provision EC2 VMs.
- Open Service Broker for Azure: This Service Broker implements support for Azure cloud services.
- Open Service Broker for Huawei Cloud: This Service Broker implements support for Huawei cloud services.
- Cf-redis-broker A service broker for a shared redis cluster.
- mongodb-open-service-broker A service broker for a mongodb cluster
- cf-mysql-broker A service broker for a shared mariadb cluster
- MySQL Java Broker: A Java port of the Ruby-based MySQL broker.MySQL Java Broker: A Java port of the Ruby-based MySQL broker.

## Terminology
- Service Broker: Service Brokers manage the lifecycle of Services. Platforms interact with Service Brokers to provision, and manage, Service Instances and Service Bindings.
- Service Offering: The advertisement of a Service that a Service Broker supports.
- Service Plan: The representation of the costs and benefits for a given variant of the Service Offering, potentially as a tier.
- Service Instance: An instantiation of a Service Offering and Service Plan.
- Service Binding: Represents the request to use a Service Instance. As part of this request there might be a reference to the entity, also known as the Application, that will use the Service Instance. 

- Service Bindings will often contain the credentials that can then be used to communicate with the Service Instance


sbt builds the compiler's input list as `sources = unmanagedSources ++ managedSources`, where `managedSources` is defined as the joined return values of every task registered in `sourceGenerators`. Registering `generate` there means sources cannot be computed until `generate` has finished, and the list it returns becomes the sources.

`Managed = produced by a task and declared to sbt. Unmanaged = put there by a human and discovered by sbt looking in a conventional directory.`


| Category | Unmanaged (found by globbing) | Managed (returned by a task) |
| --- | --- | --- |
| Sources | *unmanagedSourceDirectories* → *src/main/scala*, *src/main/java* | outputs of sourceGenerators |
| Resources | *unmanagedResourceDirectories* → *src/main/resources* | outputs of resourceGenerators |
| Classpath | *unmanagedBase* → *jars dropped in lib/* | *libraryDependencies* resolved by coursier |


```sh
root / Compile / unmanagedSources    6 files   src/main/scala/{Hello,service/*,domain/*}.scala
root / Compile / managedSources      1 file    target/.../src_managed/main/sbt-buildinfo/BuildInfo.scala
root / Compile / unmanagedResources  1 file    src/main/resources/migrations/V1__broker.sql
root / Compile / managedResources    0
```

Managed is whatever the generator task returns, with no filter, which is why the generator emitting `project/plugins.sbt` handed it straight to dotty. Managed output is disposable: the convention is to write it under `sourceManaged` (`target/…/src_managed`), so `clean/cleanFull` deletes it and the generator recreates it. sbt never deletes unmanaged files.

`sourceGenerators : SettingKey[Seq[Task[Seq[File]]]]`

The textbook example — a task that writes a file and returns it:

```scala
Compile / sourceGenerators += Def.task {
  val f = (Compile / sourceManaged).value / "Version.scala"
  IO.write(f, """object Version { val value = "1.0" }""")
  Seq(f)                       // ← the return value is what gets compiled
}.taskValue
```

`.taskValue` vs `.value` is just types:
```sh
Compile / sourceGenerators += generate.taskValue  // Task[Seq[File]] ✓  (the list wants tasks)
Compile / sourceGenerators += generate.value      // Seq[File]       ✗  type error
```

- `sources = unmanagedSources ++ managedSources`, and `managedSources` is defined as the output of the generator tasks
- `generate` returns 33 paths → `sources = [] ++ those 33`
- managedSources are managed by the build tool — sbt owns the file's lifecycle

```sh
unmanagedBase
[info] ../Projects//service-broker/lib

unmanagedResourceDirectories
[info] * /Projects/service-broker/src/main/resources

sbt:service-broker> show unmanagedSourceDirectories
[info] * /Projects/service-broker/src/main/scala
[info] * /Projects/service-broker/src/main/scala-3
[info] * /Projects/service-broker/src/main/java
sbt:service-broker> show unmanagedSources
[info] * /Projects/service-broker/src/main/scala/Hello.scala
[info] * /Projects/service-broker/src/main/scala/service/CatalogService.scala
[info] * /Projects/service-broker/src/main/scala/service/ServiceBindingsService.scala
[info] * /Projects/service-broker/src/main/scala/service/ServiceInstancesService.scala
[info] * /Projects/service-broker/src/main/scala/domain/BrokerError.scala
[info] * /Projects/service-broker/src/main/scala/domain/package.scala
[success] elapsed time: 0 s
```
```sh
show managedSources
[info] * /Projects/service-broker/target/out/jvm/scala-3.3.8/service-broker/src_managed/main/sbt-buildinfo/BuildInfo.scala

show managedSourceDirectories
[info] * /Projects/service-broker/target/out/jvm/scala-3.3.8/service-broker/src_managed/main
```

Which is where your codegen module deliberately bends the convention: those 33 files are managed — a task declares them, and their list is the authority on what compiles — but they live at `src/main/scala`, an unmanaged address, so that they're visible in the editor and committable to git. The cost of that choice is that sbt can no longer clean them up for you (`cleanFull` leaves them), and it's why the module has to explicitly switch off the unmanaged glob that would otherwise claim the same directory.

```sh
sbt:service-broker> show open-service-broker-api-codegen/Compile/unmanagedSourceDirectories
[info] * 
sbt:service-broker> show open-service-broker-api-codegen/Compile/unmanagedSources
[info] * 
[success] elapsed time: 0 s
```

```sh
Compile / sourceGenerators : SettingKey[Seq[Task[Seq[File]]]]   // a list of producers
Compile / sources          : TaskKey[Seq[File]]                 // the resulting list of files
```
```sh
sourceGenerators   (setting: Seq[Task[Seq[File]]])
      │ run every task, flatten the results
      ▼
managedSources     (task: Seq[File])  ─┐
                                       ├──► sources ──► compile
unmanagedSourceDirectories             │
      │ glob, filtered by includeFilter│
      ▼                                │
unmanagedSources   (task: Seq[File])  ─┘
```
```sbt
managedSources := generate(sourceGenerators).value
sources        := Classpaths.concatDistinct(unmanagedSources, managedSources).value

def generate(generators: SettingKey[Seq[Task[Seq[File]]]]) = generators { _.join.map(_.flatten) }
```
`sourceGenerators `is one of the two feeds into `sources`

Your `codegen` module — `openApiOutputDir := baseDirectory / "src/main/scala"`, so the generated files land at the unmanaged address:

```sh
open-service-broker-api-codegen/src/main/scala
   ↑ the generator writes here          ← managed channel returns these 33 paths
   ↑ the glob also reads here           ← unmanaged channel would find the same 33
```   

```sh
unmanagedSources / sourceDirectories → unmanagedSourceDirectories.map(d => Globs(d.toPath, recursive = true, filter))
unmanagedSources                     → those globs, executed
```