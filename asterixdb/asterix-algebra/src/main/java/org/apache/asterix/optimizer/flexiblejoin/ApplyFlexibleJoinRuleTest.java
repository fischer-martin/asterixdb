/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.optimizer.flexiblejoin;

import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalOperatorTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FlexibleJoinWrapperFunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractAssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ApplyFlexibleJoinRuleTest implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.ASSIGN) {
            return false;
        }

        AbstractAssignOperator assignOp = (AbstractAssignOperator) op;
        Mutable<ILogicalExpression> FunctionRef = assignOp.getExpressions().get(0);
        ILogicalExpression joinCondition = FunctionRef.getValue();

        if (joinCondition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression fjFuncExpr = (AbstractFunctionCallExpression) joinCondition;
        if (BuiltinFunctions.SCALAR_FJ_SUMMARY_ONE == fjFuncExpr.getFunctionIdentifier()) {
            FlexibleJoinWrapperFunctionIdentifier functionIdentifier =
                    (FlexibleJoinWrapperFunctionIdentifier) fjFuncExpr.getFunctionIdentifier();
            functionIdentifier.setLibraryName("test");
        }
        return false;
    }

    private static ReplicateOperator createReplicateOperator(Mutable<ILogicalOperator> inputOperator,
            IOptimizationContext context, SourceLocation sourceLocation, int outputArity) throws AlgebricksException {
        ReplicateOperator replicateOperator = new ReplicateOperator(outputArity);
        replicateOperator.setPhysicalOperator(new ReplicatePOperator());
        replicateOperator.setSourceLocation(sourceLocation);
        replicateOperator.getInputs().add(new MutableObject<>(inputOperator.getValue()));
        OperatorManipulationUtil.setOperatorMode(replicateOperator);
        replicateOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(replicateOperator);
        return replicateOperator;
    }

    private static ExchangeOperator createRandomPartitionExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setSourceLocation(sourceLocation);
        exchangeOperator.setPhysicalOperator(new RandomPartitionExchangePOperator(context.getComputationNodeDomain()));
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static ExchangeOperator createOneToOneExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setSourceLocation(sourceLocation);
        exchangeOperator.setPhysicalOperator(new OneToOneExchangePOperator());
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createLocalAndGlobalAggregateOperators(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> exchToLocalAggRef) throws AlgebricksException {
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        fields.add(new MutableObject<>(inputVarRef));

        // Create local aggregate operator
        IFunctionInfo localAggFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.FJ_SUMMARY_ONE);
        AggregateFunctionCallExpression localAggExpr = new AggregateFunctionCallExpression(localAggFunc, false, fields);
        localAggExpr.setSourceLocation(op.getSourceLocation());
        localAggExpr.setOpaqueParameters(new Object[] {});
        List<LogicalVariable> localAggResultVars = new ArrayList<>(1);
        List<Mutable<ILogicalExpression>> localAggFuncs = new ArrayList<>(1);
        LogicalVariable localOutVariable = context.newVar();
        localAggResultVars.add(localOutVariable);
        localAggFuncs.add(new MutableObject<>(localAggExpr));
        AggregateOperator localAggOperator = createAggregate(localAggResultVars, false, localAggFuncs,
                exchToLocalAggRef, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> localAgg = new MutableObject<>(localAggOperator);

        // Output of local aggregate operator is the input of global aggregate operator
        return createGlobalAggregateOperator(op, context, localOutVariable, localAgg);
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createGlobalAggregateOperator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> inputOperator) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> globalAggFuncArgs = new ArrayList<>(1);
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        globalAggFuncArgs.add(new MutableObject<>(inputVarRef));
        IFunctionInfo globalAggFunc =
                context.getMetadataProvider().lookupFunction(BuiltinFunctions.GLOBAL_FJ_SUMMARY_ONE);
        AggregateFunctionCallExpression globalAggExpr =
                new AggregateFunctionCallExpression(globalAggFunc, true, globalAggFuncArgs);
        globalAggExpr.setStepOneAggregate(globalAggFunc);
        globalAggExpr.setStepTwoAggregate(globalAggFunc);
        globalAggExpr.setSourceLocation(op.getSourceLocation());
        globalAggExpr.setOpaqueParameters(new Object[] {});
        List<LogicalVariable> globalAggResultVars = new ArrayList<>(1);
        globalAggResultVars.add(context.newVar());
        List<Mutable<ILogicalExpression>> globalAggFuncs = new ArrayList<>(1);
        globalAggFuncs.add(new MutableObject<>(globalAggExpr));
        AggregateOperator globalAggOperator = createAggregate(globalAggResultVars, true, globalAggFuncs, inputOperator,
                context, op.getSourceLocation());
        globalAggOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(globalAggOperator);
        MutableObject<ILogicalOperator> globalAgg = new MutableObject<>(globalAggOperator);
        return new Pair<>(globalAgg, globalAggResultVars);
    }

    private static Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> createSummary(
            AbstractBinaryJoinOperator op, IOptimizationContext context, Mutable<ILogicalOperator> inputOp,
            LogicalVariable inputVar) throws AlgebricksException {
        // Add ReplicationOperator for the input branch
        SourceLocation sourceLocation = op.getSourceLocation();
        ReplicateOperator replicateOperator = createReplicateOperator(inputOp, context, sourceLocation, 2);

        // Create one to one exchange operators for the replicator of the input branch
        ExchangeOperator exchToForward = createRandomPartitionExchangeOp(replicateOperator, context, sourceLocation);
        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchToForward);

        ExchangeOperator exchToLocalAgg = createOneToOneExchangeOp(replicateOperator, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> exchToLocalAggRef = new MutableObject<>(exchToLocalAgg);

        // Materialize the data to be able to re-read the data again
        replicateOperator.getOutputMaterializationFlags()[0] = true;

        Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createLocalAndGlobalAggResult =
                createLocalAndGlobalAggregateOperators(op, context, inputVar, exchToLocalAggRef);
        return new Triple<>(createLocalAndGlobalAggResult.first, createLocalAndGlobalAggResult.second,
                exchToForwardRef);
    }

    /**
     * Creates an aggregate operator. $$resultVariables = expressions()
     * @param resultVariables the variables which stores the result of the aggregation
     * @param isGlobal whether the aggregate operator is a global or local one
     * @param expressions the aggregation functions desired
     * @param inputOperator the input op that is feeding the aggregate operator
     * @param context optimization context
     * @param sourceLocation source location
     * @return an aggregate operator with the specified information
     * @throws AlgebricksException when there is error setting the type environment of the newly created aggregate op
     */
    private static AggregateOperator createAggregate(List<LogicalVariable> resultVariables, boolean isGlobal,
            List<Mutable<ILogicalExpression>> expressions, MutableObject<ILogicalOperator> inputOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        AggregateOperator aggregateOperator = new AggregateOperator(resultVariables, expressions);
        aggregateOperator.setPhysicalOperator(new AggregatePOperator());
        aggregateOperator.setSourceLocation(sourceLocation);
        aggregateOperator.getInputs().add(inputOperator);
        aggregateOperator.setGlobal(isGlobal);
        if (!isGlobal) {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
        } else {
            aggregateOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        }
        aggregateOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(aggregateOperator);
        return aggregateOperator;
    }

}
