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

import org.apache.asterix.common.annotations.SpatialJoinAnnotation;
import org.apache.asterix.om.functions.BuiltinFunctionInfo;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.*;
import org.apache.hyracks.algebricks.core.algebra.expressions.*;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.*;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;
import org.apache.hyracks.api.exceptions.SourceLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;


public class ApplyFlexibleJoinRule implements IAlgebraicRewriteRule {

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        return false;
    }

    @Override
    public boolean rewritePost(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {

        AbstractLogicalOperator op = (AbstractLogicalOperator) opRef.getValue();
        if (op.getOperatorTag() != LogicalOperatorTag.INNERJOIN) {
            return false;
        }

        AbstractBinaryJoinOperator joinOp = (AbstractBinaryJoinOperator) op;
        Mutable<ILogicalExpression> joinConditionRef = joinOp.getCondition();
        ILogicalExpression joinCondition = joinConditionRef.getValue();

        if (joinCondition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression stFuncExpr = (AbstractFunctionCallExpression) joinCondition;
        if (!BuiltinFunctions.isFlexibleJoinCallerFunction(stFuncExpr.getFunctionIdentifier())) {
            return false;
        }

        // Left and right arguments of the refine function should be either variable or function call.
        List<Mutable<ILogicalExpression>> stFuncArgs = stFuncExpr.getArguments();
        Mutable<ILogicalExpression> stFuncLeftArg = stFuncArgs.get(0);
        Mutable<ILogicalExpression> stFuncRightArg = stFuncArgs.get(1);

        if (stFuncLeftArg.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT
                || stFuncRightArg.getValue().getExpressionTag() == LogicalExpressionTag.CONSTANT) {
            return false;
        }



        // Gets both input branches of the spatial join.
        Mutable<ILogicalOperator> leftInputOp = op.getInputs().get(0);
        Mutable<ILogicalOperator> rightInputOp = op.getInputs().get(1);

        // Extract left and right variable of the predicate
        LogicalVariable spatialJoinVar0 = ((VariableReferenceExpression) stFuncLeftArg.getValue()).getVariableReference();
        LogicalVariable spatialJoinVar1 = ((VariableReferenceExpression) stFuncRightArg.getValue()).getVariableReference();

        LogicalVariable leftInputVar;
        LogicalVariable rightInputVar;
        Collection<LogicalVariable> liveVars = new HashSet<>();
        VariableUtilities.getLiveVariables(leftInputOp.getValue(), liveVars);
        if (liveVars.contains(spatialJoinVar0)) {
            leftInputVar = spatialJoinVar0;
            rightInputVar = spatialJoinVar1;
        } else {
            leftInputVar = spatialJoinVar1;
            rightInputVar = spatialJoinVar0;
        }

        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> leftSummarizer =
                createSummary(joinOp, context, leftInputOp, leftInputVar);
        MutableObject<ILogicalOperator> leftGlobalAgg = leftSummarizer.first;
        List<LogicalVariable> leftGlobalAggResultVars = leftSummarizer.second;
        MutableObject<ILogicalOperator> leftExchToJoinOpRef = leftSummarizer.third;
        LogicalVariable leftSummary = leftGlobalAggResultVars.get(0);

        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> rightSummarizer =
                createSummary(joinOp, context, rightInputOp, rightInputVar);
        MutableObject<ILogicalOperator> rightGlobalAgg = rightSummarizer.first;
        List<LogicalVariable> rightGlobalAggResultVars = rightSummarizer.second;
        MutableObject<ILogicalOperator> rightExchToJoinOpRef = rightSummarizer.third;
        LogicalVariable rightSummary = leftGlobalAggResultVars.get(0);


        return true;

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
        IFunctionInfo globalAggFunc = context.getMetadataProvider().lookupFunction(BuiltinFunctions.GLOBAL_FJ_SUMMARY_ONE);
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
