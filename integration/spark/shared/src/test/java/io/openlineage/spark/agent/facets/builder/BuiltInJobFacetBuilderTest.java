/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.builtin.scala.v1.FacetEmitter$;
import java.util.function.BiConsumer;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BuiltInJobFacetBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);
  BuiltInJobFacetBuilder builder = new BuiltInJobFacetBuilder(context);

  @BeforeEach
  void setup() {
    FacetEmitter$.MODULE$.clear();
  }

  @Test
  void testIsDefinedWhenNoFacetsRegistered() {
    assertThat(builder.isDefinedAt(mock(LogicalPlan.class))).isFalse();
  }

  @Test
  void testIsDefined() {
    FacetEmitter$.MODULE$.emitJobFacet("some-facet", mock(JobFacet.class));
    assertThat(builder.isDefinedAt(mock(LogicalPlan.class))).isTrue();
  }

  @Test
  void testApply() {
    BiConsumer<String, ? super JobFacet> consumer = mock(BiConsumer.class);

    JobFacet facet1 = mock(JobFacet.class);
    JobFacet facet2 = mock(JobFacet.class);

    FacetEmitter$.MODULE$.emitJobFacet("facet1", facet1);
    FacetEmitter$.MODULE$.emitJobFacet("facet2", facet2);

    builder.build(mock(LogicalPlan.class), consumer);

    verify(consumer, times(1)).accept("facet1", facet1);
    verify(consumer, times(1)).accept("facet2", facet2);
  }
}
