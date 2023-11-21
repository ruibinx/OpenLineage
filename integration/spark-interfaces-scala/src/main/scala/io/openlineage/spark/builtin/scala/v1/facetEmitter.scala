/** Copyright 2018-2024 contributors to the OpenLineage project
  * SPDX-License-Identifier: Apache-2.0
  */
package io.openlineage.spark.builtin.scala.v1

import io.openlineage.client.OpenLineage.{JobFacet, RunFacet}

import java.util

/** Companion object to store run and job facets to be included in the next sent
  * event. Facets are lazily emitted next Openlineage event is emitted.
  */
object FacetEmitter {
  // Java maps are used to avoid Scala version mismatch
  var runFacets: java.util.Map[String, RunFacet] = new util.HashMap()
  var jobFacets: java.util.Map[String, JobFacet] = new util.HashMap()
  var openlineageContext: Option[OpenLineageContext] = Option.empty
  var isTerminatingEventSent: Boolean = false

  def registerContext(openLineageContext: OpenLineageContext): Unit = {
    FacetEmitter.openlineageContext = Option.apply(openLineageContext);
    isTerminatingEventSent = false
  }

  def emitRunFacet(name: String, facet: RunFacet): Unit = {
    runFacets.put(name, facet)
  }

  def emitJobFacet(name: String, facet: JobFacet): Unit = {
    jobFacets.put(name, facet)
  }

  def markTerminatingEventSent(): Unit = {
    isTerminatingEventSent = true
  }

  def clear(): Unit = {
    runFacets.clear()
    jobFacets.clear()
  }
}
