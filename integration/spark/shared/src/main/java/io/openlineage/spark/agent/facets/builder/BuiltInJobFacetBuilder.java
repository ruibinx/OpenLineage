/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.spark.agent.util.BuiltInPlanUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.builtin.scala.v1.FacetEmitter$;
import java.util.function.BiConsumer;

/** Builder to extract job facets from extensions via `FacetRegistry` static object. */
public class BuiltInJobFacetBuilder extends CustomFacetBuilder<Object, JobFacet> {

  private final OpenLineageContext openLineageContext;

  public BuiltInJobFacetBuilder(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;

    FacetEmitter$.MODULE$.registerContext(BuiltInPlanUtils.context(openLineageContext));
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return FacetEmitter$.MODULE$.jobFacets() != null
        && !FacetEmitter$.MODULE$.jobFacets().isEmpty();
  }

  @Override
  protected void build(Object event, BiConsumer<String, ? super JobFacet> consumer) {
    FacetEmitter$.MODULE$.jobFacets().forEach((name, facet) -> consumer.accept(name, facet));
  }
}
