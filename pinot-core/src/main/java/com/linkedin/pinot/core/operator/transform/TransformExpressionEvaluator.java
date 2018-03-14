/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.transform;

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import java.util.Map;


/**
 * Interface for Transform expressions evaluation.
 *
 */
public interface TransformExpressionEvaluator {

  /**
   * Evaluates a set of expression trees on a given block of data.
   *
   * @param projectionBlock Projection block for which to evaluate the expression trees for
   * @return Map from expression tree to the result after evaluation
   */
  Map<TransformExpressionTree, BlockValSet> evaluate(ProjectionBlock projectionBlock);
}
