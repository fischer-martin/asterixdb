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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.utils.ListSet;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalExpressionTag;
import org.apache.hyracks.algebricks.core.algebra.base.LogicalVariable;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AbstractLogicalExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.AggregateFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ConstantExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.UnnestingFunctionCallExpression;
import org.apache.hyracks.algebricks.core.algebra.expressions.VariableReferenceExpression;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractBinaryJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AbstractLogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AggregateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.AssignOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ExchangeOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.InnerJoinOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ProjectOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.ReplicateOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.UnnestOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.logical.visitors.VariableUtilities;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AbstractJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AggregatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.AssignPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.BroadcastExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.UnnestPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ApplyFlexibleJoinUtils {

    public static boolean tryFlexibleJoin(AbstractBinaryJoinOperator joinOp, IOptimizationContext context,
            ILogicalExpression joinCondition, int left, int right) throws AlgebricksException {

        if (joinCondition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression stFuncExpr = (AbstractFunctionCallExpression) joinCondition;
        if (!BuiltinFunctions.isFlexibleJoinCallerFunction(stFuncExpr.getFunctionIdentifier())) {
            return false;
        }
        // Extracts spatial intersect function's arguments
        List<Mutable<ILogicalExpression>> joinArgs = stFuncExpr.getArguments();
        /*if (joinArgs.size() != 2) {
            return false;
        }*/

        ILogicalExpression flexibleJoinLeftArg = joinArgs.get(left).getValue();
        ILogicalExpression flexibleJoinRightArg = joinArgs.get(right).getValue();

        // Left and right arguments of the spatial_intersect function should be variables
        if (flexibleJoinLeftArg.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || flexibleJoinRightArg.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }

        // Gets both input branches of the spatial join.
        Mutable<ILogicalOperator> leftInputOp = joinOp.getInputs().get(left);
        Mutable<ILogicalOperator> rightInputOp = joinOp.getInputs().get(right);

        // Extract left and right variable of the predicate
        LogicalVariable spatialJoinVar0 = ((VariableReferenceExpression) flexibleJoinLeftArg).getVariableReference();
        LogicalVariable spatialJoinVar1 = ((VariableReferenceExpression) flexibleJoinRightArg).getVariableReference();

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

        String libraryName = "";

        if(stFuncExpr.getFunctionIdentifier().equals(BuiltinFunctions.CUSTOM_TEXT_FUNCTION)) {
            libraryName = "org.apache.asterix.runtime.flexiblejoin.SetSimilarityJoin";
        } else {
            libraryName = "org.apache.asterix.runtime.flexiblejoin.SpatialJoin";
        }

        //Add a dynamic workflof for the summary one
        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> leftSummarizer =
                createSummary(joinOp, context, leftInputOp, leftInputVar, 0);
        MutableObject<ILogicalOperator> leftGlobalAgg = leftSummarizer.first;
        List<LogicalVariable> leftGlobalAggResultVars = leftSummarizer.second;
        MutableObject<ILogicalOperator> leftExchToJoinOpRef = leftSummarizer.third;
        LogicalVariable leftSummary = leftGlobalAggResultVars.get(0);

        //Add a dynamic workflof for the summary two
        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> rightSummarizer =
                createSummary(joinOp, context, rightInputOp, rightInputVar, 1);
        MutableObject<ILogicalOperator> rightGlobalAgg = rightSummarizer.first;
        List<LogicalVariable> rightGlobalAggResultVars = rightSummarizer.second;
        MutableObject<ILogicalOperator> rightExchToJoinOpRef = rightSummarizer.third;
        LogicalVariable rightSummary = rightGlobalAggResultVars.get(0);

        // Join the left and right summaries
        Mutable<ILogicalExpression> trueCondition =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
        InnerJoinOperator unionMBRJoinOp = new InnerJoinOperator(trueCondition, leftGlobalAgg, rightGlobalAgg);
        unionMBRJoinOp.setSourceLocation(joinOp.getSourceLocation());
        unionMBRJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        MutableObject<ILogicalOperator> divideJoinRef = new MutableObject<>(unionMBRJoinOp);
        unionMBRJoinOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(unionMBRJoinOp);

        //Call the divide function to create the Configuration
        BuiltinFunctions.FJ_DIVIDE.setLibraryName(libraryName);
        IFunctionInfo DivideFunctionInfo = context.getMetadataProvider().lookupFunction(BuiltinFunctions.FJ_DIVIDE);

        List<Mutable<ILogicalExpression>> getDivideFuncInputExprs = new ArrayList<>();
        getDivideFuncInputExprs.add(new MutableObject<>(new VariableReferenceExpression(leftSummary)));
        getDivideFuncInputExprs.add(new MutableObject<>(new VariableReferenceExpression(rightSummary)));
        ScalarFunctionCallExpression getDivideFuncExpr = new ScalarFunctionCallExpression(
                DivideFunctionInfo,
                getDivideFuncInputExprs);
        getDivideFuncExpr.setSourceLocation(joinOp.getSourceLocation());

        Mutable<ILogicalExpression> configurationExpr = new MutableObject<>(getDivideFuncExpr);
        LogicalVariable configuration = context.newVar();
        AbstractLogicalOperator configurationAssignOperator =
                new AssignOperator(configuration, configurationExpr);
        configurationAssignOperator.setSourceLocation(joinOp.getSourceLocation());
        configurationAssignOperator.setExecutionMode(joinOp.getExecutionMode());
        configurationAssignOperator.setPhysicalOperator(new AssignPOperator());
        configurationAssignOperator.getInputs().add(new MutableObject<>(divideJoinRef.getValue()));
        context.computeAndSetTypeEnvironmentForOperator(configurationAssignOperator);
        configurationAssignOperator.recomputeSchema();
        MutableObject<ILogicalOperator> configurationAssignOperatorRef =
                new MutableObject<>(configurationAssignOperator);

        ReplicateOperator configReplicateOperator =
                createReplicateOperator(configurationAssignOperatorRef, context, joinOp.getSourceLocation(), 3);

        // Replicate configuration to the left branch
        ExchangeOperator exchConfigToJoinOpLeft =
                createBroadcastExchangeOp(configReplicateOperator, context, joinOp.getSourceLocation());
        MutableObject<ILogicalOperator> exchConfigToJoinOpLeftRef = new MutableObject<>(exchConfigToJoinOpLeft);
        Pair<LogicalVariable, Mutable<ILogicalOperator>> createLeftAssignProjectOperatorResult =
                createAssignProjectOperator(joinOp, configuration, configReplicateOperator,
                        exchConfigToJoinOpLeftRef, context);
        LogicalVariable leftConfigurationVar = createLeftAssignProjectOperatorResult.getFirst();
        Mutable<ILogicalOperator> leftConfigurationRef = createLeftAssignProjectOperatorResult.getSecond();

        // Replicate configuration to the right branch
        ExchangeOperator exchConfigToJoinOpRight =
                createBroadcastExchangeOp(configReplicateOperator, context, joinOp.getSourceLocation());
        MutableObject<ILogicalOperator> exchConfigToJoinOpRightRef = new MutableObject<>(exchConfigToJoinOpRight);
        Pair<LogicalVariable, Mutable<ILogicalOperator>> createRightAssignProjectOperatorResult =
                createAssignProjectOperator(joinOp, configuration, configReplicateOperator,
                        exchConfigToJoinOpRightRef, context);
        LogicalVariable rightConfigurationVar = createRightAssignProjectOperatorResult.getFirst();
        Mutable<ILogicalOperator> rightConfigurationRef = createRightAssignProjectOperatorResult.getSecond();

        // Replicate Configuration to the right branch of a later Nested Loop Join for the verify function
        ExchangeOperator exchConfigToVerifyJoinOp =
                createBroadcastExchangeOp(configReplicateOperator, context, joinOp.getSourceLocation());
        MutableObject<ILogicalOperator> exchConfigToVerifyJoinOpRef =
                new MutableObject<>(exchConfigToVerifyJoinOp);

        // Add left Join (TRUE)
        Mutable<ILogicalExpression> leftTrueCondition =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
        InnerJoinOperator leftJoinOp =
                new InnerJoinOperator(leftTrueCondition, leftExchToJoinOpRef, leftConfigurationRef);
        leftJoinOp.setSourceLocation(joinOp.getSourceLocation());
        leftJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        MutableObject<ILogicalOperator> leftJoinRef = new MutableObject<>(leftJoinOp);
        leftJoinOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(leftJoinOp);
        leftInputOp.setValue(leftJoinRef.getValue());

        // Add right Join (TRUE)
        Mutable<ILogicalExpression> rightTrueCondition =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
        InnerJoinOperator rightJoinOp =
                new InnerJoinOperator(rightTrueCondition, rightExchToJoinOpRef, rightConfigurationRef);
        rightJoinOp.setSourceLocation(joinOp.getSourceLocation());
        rightJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        MutableObject<ILogicalOperator> rightJoinRef = new MutableObject<>(rightJoinOp);
        rightJoinOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(rightJoinOp);
        rightInputOp.setValue(rightJoinRef.getValue());

        Mutable<ILogicalExpression> leftConfigurationExpr =
                new MutableObject<>(new VariableReferenceExpression(leftConfigurationVar));
        Mutable<ILogicalExpression> rightConfigurationExpr =
                new MutableObject<>(new VariableReferenceExpression(rightConfigurationVar));
        Mutable<ILogicalExpression> verifyConfigurationExpr =
                new MutableObject<>(new VariableReferenceExpression(configuration));

        // Inject unnest operator to add bucket IDs to the left and right branch of the join operator
        LogicalVariable leftBucketIdVar = ApplyFlexibleJoinUtils.injectAssignOneUnnestOperator(context, leftInputOp,
                leftInputVar, leftConfigurationExpr);
        LogicalVariable rightBucketIdVar = ApplyFlexibleJoinUtils.injectAssignTwoUnnestOperator(context, rightInputOp,
                rightInputVar, rightConfigurationExpr);


        ScalarFunctionCallExpression verifyEquiJoinCondition =
                createVerifyCondition(joinOp, verifyConfigurationExpr, leftBucketIdVar, rightBucketIdVar,
                        leftInputVar, rightInputVar);

        ScalarFunctionCallExpression updatedJoinCondition;

        /*updatedJoinCondition = (ScalarFunctionCallExpression) stFuncExpr;

        Mutable<ILogicalExpression> joinConditionRef = joinOp.getCondition();
        joinConditionRef.setValue(updatedJoinCondition);*/

        List<LogicalVariable> keysLeftBranch = new ArrayList<>();
        keysLeftBranch.add(leftBucketIdVar);
        keysLeftBranch.add(leftInputVar);

        List<LogicalVariable> keysRightBranch = new ArrayList<>();
        keysRightBranch.add(rightBucketIdVar);
        keysRightBranch.add(rightInputVar);

        BuiltinFunctions.FJ_MATCH.setLibraryName(libraryName);
        IFunctionInfo MatchFunctionInfo = context.getMetadataProvider().lookupFunction(BuiltinFunctions.FJ_MATCH);

        ScalarFunctionCallExpression match = new ScalarFunctionCallExpression(MatchFunctionInfo,
                new MutableObject<>(new VariableReferenceExpression(leftBucketIdVar)),
                new MutableObject<>(new VariableReferenceExpression(rightBucketIdVar)));

        InnerJoinOperator flexibleJoinOp =
                new InnerJoinOperator(new MutableObject<>(match), leftInputOp, rightInputOp);
        flexibleJoinOp.setSourceLocation(joinOp.getSourceLocation());
        //SpatialJoinUtils.setSpatialJoinOp(spatialJoinOp, keysLeftBranch, keysRightBranch, context);
        flexibleJoinOp.setSchema(joinOp.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(flexibleJoinOp);

        Mutable<ILogicalOperator> opRef = new MutableObject<>(joinOp);
        Mutable<ILogicalOperator> flexibleJoinOpRef = new MutableObject<>(flexibleJoinOp);

        InnerJoinOperator verifyJoinOp =
                new InnerJoinOperator(new MutableObject<>(verifyEquiJoinCondition), flexibleJoinOpRef,
                        exchConfigToVerifyJoinOpRef);
        verifyJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(
                AbstractBinaryJoinOperator.JoinKind.INNER, AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        MutableObject<ILogicalOperator> referencePointTestJoinOpRef = new MutableObject<>(verifyJoinOp);
        verifyJoinOp.setSourceLocation(joinOp.getSourceLocation());
        context.computeAndSetTypeEnvironmentForOperator(verifyJoinOp);
        verifyJoinOp.recomputeSchema();
        opRef.setValue(referencePointTestJoinOpRef.getValue());
        joinOp.getInputs().clear();
        joinOp.getInputs().addAll(verifyJoinOp.getInputs());
        joinOp.setPhysicalOperator(verifyJoinOp.getPhysicalOperator());
        joinOp.getCondition().setValue(verifyJoinOp.getCondition().getValue());
        context.computeAndSetTypeEnvironmentForOperator(joinOp);
        joinOp.recomputeSchema();

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
            MutableObject<ILogicalOperator> exchToLocalAggRef, int one) throws AlgebricksException {
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        fields.add(new MutableObject<>(inputVarRef));

        // Create local aggregate operator
        FunctionIdentifier functionIdentifier = BuiltinFunctions.LOCAL_FJ_SUMMARY_ONE;
        if(one == 1) {
            functionIdentifier = BuiltinFunctions.LOCAL_FJ_SUMMARY_TWO;
        }
        IFunctionInfo localAggFunc = context.getMetadataProvider().lookupFunction(functionIdentifier);
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
        return createGlobalAggregateOperator(op, context, localOutVariable, localAgg, one);
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createGlobalAggregateOperator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> inputOperator, int one) throws AlgebricksException {
        List<Mutable<ILogicalExpression>> globalAggFuncArgs = new ArrayList<>(1);
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        globalAggFuncArgs.add(new MutableObject<>(inputVarRef));
        FunctionIdentifier functionIdentifier = BuiltinFunctions.GLOBAL_FJ_SUMMARY_ONE;
        if(one == 1) {
            functionIdentifier = BuiltinFunctions.GLOBAL_FJ_SUMMARY_TWO;
        }
        IFunctionInfo globalAggFunc =
                context.getMetadataProvider().lookupFunction(functionIdentifier);
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
            LogicalVariable inputVar, int one) throws AlgebricksException {
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
                createLocalAndGlobalAggregateOperators(op, context, inputVar, exchToLocalAggRef, one);
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

    private static LogicalVariable injectAssignOneUnnestOperator(IOptimizationContext context,
                                                                 Mutable<ILogicalOperator> op,
                                                                 LogicalVariable unnestVar,
                                                                 Mutable<ILogicalExpression> configureExpr)
            throws AlgebricksException {
        SourceLocation srcLoc = op.getValue().getSourceLocation();
        LogicalVariable bucketIdVar = context.newVar();
        VariableReferenceExpression unnestVarRef = new VariableReferenceExpression(unnestVar);
        unnestVarRef.setSourceLocation(srcLoc);
        UnnestingFunctionCallExpression spatialTileFuncExpr = new UnnestingFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.FJ_ASSIGN_ONE),
                new MutableObject<>(unnestVarRef), configureExpr);
        spatialTileFuncExpr.setSourceLocation(srcLoc);
        UnnestOperator unnestOp = new UnnestOperator(bucketIdVar, new MutableObject<>(spatialTileFuncExpr));
        unnestOp.setPhysicalOperator(new UnnestPOperator());
        unnestOp.setSourceLocation(srcLoc);
        unnestOp.getInputs().add(new MutableObject<>(op.getValue()));
        context.computeAndSetTypeEnvironmentForOperator(unnestOp);
        unnestOp.recomputeSchema();
        op.setValue(unnestOp);
        return bucketIdVar;
    }

    private static LogicalVariable injectAssignTwoUnnestOperator(IOptimizationContext context,
                                                                 Mutable<ILogicalOperator> op,
                                                                 LogicalVariable unnestVar,
                                                                 Mutable<ILogicalExpression> configureExpr)
            throws AlgebricksException {
        SourceLocation srcLoc = op.getValue().getSourceLocation();
        LogicalVariable bucketIdVar = context.newVar();
        VariableReferenceExpression unnestVarRef = new VariableReferenceExpression(unnestVar);
        unnestVarRef.setSourceLocation(srcLoc);
        UnnestingFunctionCallExpression spatialTileFuncExpr = new UnnestingFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.FJ_ASSIGN_TWO),
                new MutableObject<>(unnestVarRef), configureExpr);
        spatialTileFuncExpr.setSourceLocation(srcLoc);
        UnnestOperator unnestOp = new UnnestOperator(bucketIdVar, new MutableObject<>(spatialTileFuncExpr));
        unnestOp.setPhysicalOperator(new UnnestPOperator());
        unnestOp.setSourceLocation(srcLoc);
        unnestOp.getInputs().add(new MutableObject<>(op.getValue()));
        context.computeAndSetTypeEnvironmentForOperator(unnestOp);
        unnestOp.recomputeSchema();
        op.setValue(unnestOp);
        return bucketIdVar;
    }
    private static ExchangeOperator createBroadcastExchangeOp(ReplicateOperator replicateOperator,
                                                              IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setSourceLocation(sourceLocation);
        exchangeOperator.setPhysicalOperator(new BroadcastExchangePOperator(context.getComputationNodeDomain()));
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static Pair<LogicalVariable, Mutable<ILogicalOperator>> createAssignProjectOperator(
            AbstractBinaryJoinOperator op, LogicalVariable inputVar, ReplicateOperator replicateOperator,
            MutableObject<ILogicalOperator> exchMBRToForwardRef, IOptimizationContext context)
            throws AlgebricksException {
        LogicalVariable newFinalMbrVar = context.newVar();
        List<LogicalVariable> finalMBRLiveVars = new ArrayList<>();
        finalMBRLiveVars.add(newFinalMbrVar);
        ListSet<LogicalVariable> finalMBRLiveVarsSet = new ListSet<>();
        finalMBRLiveVarsSet.add(newFinalMbrVar);

        Mutable<ILogicalExpression> finalMBRExpr = new MutableObject<>(new VariableReferenceExpression(inputVar));
        AbstractLogicalOperator assignOperator = new AssignOperator(newFinalMbrVar, finalMBRExpr);
        assignOperator.setSourceLocation(op.getSourceLocation());
        assignOperator.setExecutionMode(replicateOperator.getExecutionMode());
        assignOperator.setPhysicalOperator(new AssignPOperator());
        AbstractLogicalOperator projectOperator = new ProjectOperator(finalMBRLiveVars);
        projectOperator.setSourceLocation(op.getSourceLocation());
        projectOperator.setPhysicalOperator(new StreamProjectPOperator());
        projectOperator.setExecutionMode(replicateOperator.getExecutionMode());
        assignOperator.getInputs().add(exchMBRToForwardRef);
        projectOperator.getInputs().add(new MutableObject<ILogicalOperator>(assignOperator));

        context.computeAndSetTypeEnvironmentForOperator(assignOperator);
        assignOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(projectOperator);
        projectOperator.recomputeSchema();
        Mutable<ILogicalOperator> projectOperatorRef = new MutableObject<>(projectOperator);

        return new Pair<>(newFinalMbrVar, projectOperatorRef);
    }

    private static ScalarFunctionCallExpression createVerifyCondition(AbstractBinaryJoinOperator op,
                                                                      Mutable<ILogicalExpression> referencePointTestMBRExpr, LogicalVariable leftTileIdVar,
                                                                      LogicalVariable rightTileIdVar, LogicalVariable leftInputVar, LogicalVariable rightInputVar) {
        // Compute reference tile ID
        ScalarFunctionCallExpression verify = new ScalarFunctionCallExpression(
                BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.FJ_VERIFY),
                new MutableObject<>(new VariableReferenceExpression(leftTileIdVar)),
                new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
                new MutableObject<>(new VariableReferenceExpression(rightTileIdVar)),
                new MutableObject<>(new VariableReferenceExpression(rightInputVar)),
                referencePointTestMBRExpr

        );
        verify.setSourceLocation(op.getSourceLocation());


        return verify;
    }
}
