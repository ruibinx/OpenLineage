/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan.handlers;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.BuiltInPlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.builtin.scala.v1.LineageRelation;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

public class BuiltInLineageV1RelationHandler<D extends Dataset> {
  private final DatasetFactory<D> datasetFactory;
  private final OpenLineageContext context;

  public BuiltInLineageV1RelationHandler(
      OpenLineageContext context, DatasetFactory<D> datasetFactory) {
    this.datasetFactory = datasetFactory;
    this.context = context;
  }

  public List<D> handleRelation(LogicalRelation x) {
    if (!(x.relation() instanceof LineageRelation)) {
      return Collections.emptyList();
    }

    LineageRelation relation = (LineageRelation) x.relation();
    DatasetIdentifier di = relation.getLineageDatasetIdentifier(BuiltInPlanUtils.context(context));

    if (x.schema() != null) {
      return Collections.singletonList(datasetFactory.getDataset(di, x.schema()));
    } else {
      return Collections.singletonList(
          datasetFactory.getDataset(di, context.getOpenLineage().newDatasetFacetsBuilder()));
    }
  }
}
