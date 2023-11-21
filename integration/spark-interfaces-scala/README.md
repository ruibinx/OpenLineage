# Spark Built-in package

Spark ecosystem comes with a plenty of extensions that affect lineage extraction logic.
This package contains Scala traits which can be implemented on the extension's side to
generate high quality metadata for OpenLineage events. 

## Problem definition

Openlineage Spark integration is based on `openlineage-spark.jar` library attached 
to a running Spark job. The library traverses Spark logical plan on run state updates to generate
Openlineage events. While traversing plan's tree, the library extracts input and output datasets 
as well as other interesting aspects of this particular job, run or datasets involved in the processing.
Extraction code for each node is contained within `openlineage-spark.jar`.

Two main issues with this approach are: 
 * Spark ecosystem comes with plenty of extensions and many of them add 
custom nodes into the logical plan of the query executed. 
These nodes need to be traversed and understood by `openlineage-spark` to
extract lineage out of them. This brings serious complexity to the code base. Not only openlineage
has to cover multiple Spark versions, but also each Spark version supports multiple versions of
multiple extensions. 

 * Spark extensions know a lot of valuable metadata that can be published within OpenLineage events. 
It makes sense to allow extensions publish facets on their own. This [issue](https://github.com/OpenLineage/OpenLineage/issues/167)
contains great example of useful aspects that can be retrieved from Iceberg extension. 

## Solution

A remedy to the problems above is to migrate lineage extraction logic directly to 
Spark `LogicalPlan` nodes. The advantages of this approach are: 
 * **One-to-one version matching** - there is no further need for a single integration code to support
multiple versions of a Spark extension. 
 * **Avoid breaking changes** - this approach limits amount of upgrades that break integration between
`openlineage-spark` and other extensions, as lineage extraction code is directly put into extensions
codebase which assures that changes on the Spark extension side are not breaking it. 
 * **Ability to publish extension related lineage metadata** - tighter integration level allows Spark
extensions to publish more extension-specific lineage metadata.

`spark-interfaces-scala` package contains traits that shall be implemented as well as extra utility 
classes to let integrate OpenLineage within any Spark extension. 

Package code should not be shipped with extension that implements traits. Dependency should be marked
as compile-only. Implementation of the code calling the methods should be responsible for providing
`spark-interfaces-scala` on the classpath. 

Please note that this package as well as the traits should be considered experimental and may evolve
in the future. All the current logic has been put into `*.scala.v1` package. First, it is possible
we develop the same interfaces in Java. Secondly, in case of non-compatible changes, 
we are going to release `v2` interfaces. We're aiming to support different versions within spark
integration. 

## Extracting datasets from LogicalPlan nodes

Two interfaces have been prepared:
 * `io.openlineage.spark.builtin.scala.v1.InputLineageNode` with `getInputs` method,
 * `io.openlineage.spark.builtin.scala.v1.InputLineageNode` with `getOutputs` method.

They return list of `InputDatasetWithFacets` and `OutputDatasetWithFacets` respectively. Each trait has methods 
to expose dataset facets as well facets that relate to particular dataset only in the context of 
current run, like amount of bytes read from a certain dataset. 

There are two possible ways to identify dataset returned by a trait. The straightforward one
is to identify it by `namespace` as `name` strings, which is a classical dataset identifier within
OpenLineage convention. 
Oftentimes this approach can be cumbersome. 
There are many `command` nodes whose output dataset is `DatasourceV2Relation`, which is another 
node within the logical plan. In this case, it makes 
sense to delegate dataset extraction to other node, while still having the ability to enrich it
with the facets of the current node. For this scenario, case classes `InputDatasetWithDelegate` and 
`OutputDatasetWithDelegate` have been created. They allow assigning facets to a dataset, while
still letting other code to extract metadata for the same dataset. The classes contain `node` object
property which defines node within logical plan to contain more metadata about the dataset. 

An example implementation for `ReplaceIcebergData` node:

```scala
override def getOutputs(context: OpenLineageContext): List[OutputDatasetWithFacets] = {
    if (!table.isInstanceOf[DataSourceV2Relation]) {
      List()
    } else {
      val relation = table.asInstanceOf[DataSourceV2Relation]
      val datasetFacetsBuilder: DatasetFacetsBuilder = {
        new OpenLineage.DatasetFacetsBuilder()
          .lifecycleStateChange(
          context
            .openLineage
            .newLifecycleStateChangeDatasetFacet(
              OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE,
              null
            )
        )
      }
      
      // enrich dataset with additional facets like a dataset version
      DatasetVersionUtils.getVersionOf(relation) match {
        case Some(version) => datasetFacetsBuilder.version(
          context
            .openLineage
            .newDatasetVersionDatasetFacet(version)
        )
        case None =>
      }

      // return output dataset while pointing that more dataset details shall be extracted from
      // `relation` object.
      List(
        OutputDatasetWithDelegate(
          relation,
          datasetFacetsBuilder,
          new OpenLineage.OutputDatasetOutputFacetsBuilder()
        )
      )
    }
  }
```

Sometimes an extension can implement its own `org.apache.spark.sql.sources.BaseRelation` or
`org.apache.spark.sql.sources.RelationProvider` while not modifying Spark plan nodes. In such cases,
`io.openlineage.spark.builtin.scala.v1.LineageRelation` and 
`io.openlineage.spark.builtin.scala.v1.LineageDatasetProvider` traits can be used to expose
relation lineage.
 
## Run and Job Facets

The library also allows enriching OpenLineage events being sent by any job and run facets. 

`io.openlineage.spark.builtin.scala.v1.FacetEmitter` object is a static singleton which allows
registering arbitrary facets to be emitted within the next OpenLineage event. 

This can be done with methods:
 * `def emitRunFacet(name: String, facet: RunFacet): Unit`
 * `def emitJobFacet(name: String, facet: JobFacet): Unit`

When initializing OpenLineage, facet registry gets `OpenLineageContext` which can be used to generate
the facets. It contains `OpenLineage` object with builders for all the facets defined in the 
specification.

Please note that library gives no guarantee if the next event is going to be sent. Registering a 
facet after the job has finished will result in facets not being sent. This can be verified 
by a client code calling `io.openlineage.spark.builtin.scala.v1.FacetEmitter.isTerminatingEventSent()`.
It is left to a client library to decide what to do with such scenario. 

## Column Level Lineage

Lineage is extracted from the optimized logical plan. The plan is a tree with the root being the output
dataset and leaves the input datasets. In order to collect column level lineage we need to track dependencies between input and output fields.

Each node within plan has to understand which input attributes it consumes and how they affect output attributes produced by the node.
Attribute fields within plan are identified by `ExprId`. In order to build column level lineage,
dependencies between input and output attributes for each plan's node need to be identified. 

In order to emit column level lineage from a given spark node, `io.openlineage.spark.builtin.scala.v1.ColumnLevelLineageNode`
trait has to be implemented. The trait should implement following methods
 * `def columnLevelLineageInputs(context: OpenLineageContext): List[DatasetFieldLineage]`
 * `def columnLevelLineageOutputs(context: OpenLineageContext): List[DatasetFieldLineage]`
 * `columnLevelLineageDependencies(context: OpenLineageContext): List[ExpressionDependency]`

First two methods are used to identify input and output fields as well as matching the fields
to expressions which use the fields. Returned field lineage can contain identifier, which is mostly
field name, but can also be represented by a delegate object pointing to expression where
the identifier shall be extracted from. 

`ExpressionDependency` allows matching, for each Spark plan node, input expressions onto output
expressions. Having all the inputs and outputs identified, as well as intermediate dependencies between
the expressions used, allow building column level lineage facet.

Code below contains an example of `ColumnLevelLineageNode` within Iceberg's `MergeRows` class
that implements `MERGE INTO` for Spark 3.4:

```scala
case class MergeRows(
    ...,
    matchedOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedOutputs: Seq[Seq[Expression]],
    output: Seq[Attribute],
    child: LogicalPlan
) extends UnaryNode with ColumnLevelLineageNode {
  
    override def columnLevelLineageDependencies(context: OpenLineageContext): List[ExpressionDependency] = {
      val deps: ListBuffer[ExpressionDependency] = ListBuffer()

      // For each matched and not-matched outputs `ExpressionDependencyWithDelegate` is created
      // This means for output expression id `attr.exprId.id`, `expr` node needs to be examined to 
      // detect input expression ids. 
      output.zipWithIndex.foreach {
        case (attr: Attribute, index: Int) =>
          notMatchedOutputs
            .toStream
            .filter(exprs => exprs.size > index)
            .map(exprs => exprs(index))
            .foreach(expr => deps += ExpressionDependencyWithDelegate(OlExprId(attr.exprId.id), expr))
          matchedOutputs
            .foreach {
              matched =>
                matched
                  .toStream
                  .filter(exprs => exprs.size > index)
                  .map(exprs => exprs(index))
                  .foreach(expr => deps += ExpressionDependencyWithDelegate(OlExprId(attr.exprId.id), expr))
            }
      }

      deps.toList
    }

    override def columnLevelLineageInputs(context: OpenLineageContext): List[DatasetFieldLineage] = {
      // Delegates input field extraction to other logical plan node
      List(InputDatasetFieldFromDelegate(child))
    }

    override def columnLevelLineageOutputs(context: OpenLineageContext): List[DatasetFieldLineage] = {
      // For each output attribute return its name and ExprId assigned to it.
      // We're aiming for lineage traits to stay Spark version agnostic and don't want to rely 
      // on Spark classes. That's why `OlExprId` is used to pass `ExprId`
      output.map(a => OutputDatasetField(a.name, OlExprId(a.exprId.id))).toList
    }
  }
```

Please note that `ExpressionDependency` can be extended in the future to contain more information
on how inputs were used to produce a certain output attribute.