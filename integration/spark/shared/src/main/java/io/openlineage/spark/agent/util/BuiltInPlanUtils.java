/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.spark.api.OpenLineageContext;

public class BuiltInPlanUtils {

  public static io.openlineage.spark.builtin.scala.v1.OpenLineageContext context(
      OpenLineageContext context) {
    return () -> context.getOpenLineage();
  }
}
