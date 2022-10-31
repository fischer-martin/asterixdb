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

import org.apache.asterix.algebra.operators.physical.FlexibleJoinPOperator;
import org.apache.asterix.common.annotations.FlexibleJoinAnnotation;
import org.apache.asterix.common.functions.FunctionSignature;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Function;
import org.apache.asterix.metadata.functions.ExternalFunctionCompilerUtil;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.constants.AsterixConstantValue;
import org.apache.asterix.om.functions.BuiltinFunctions;
import org.apache.asterix.om.functions.ExternalFJFunctionInfo;
import org.apache.asterix.om.functions.FunctionInfo;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.utils.ConstantExpressionUtil;
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
import org.apache.hyracks.algebricks.core.algebra.operators.physical.HybridHashJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.NestedLoopJoinPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.OneToOneExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.RandomPartitionExchangePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.ReplicatePOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.StreamProjectPOperator;
import org.apache.hyracks.algebricks.core.algebra.operators.physical.UnnestPOperator;
import org.apache.hyracks.algebricks.core.algebra.util.OperatorManipulationUtil;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class ApplyFlexibleJoinUtils {

    private static MetadataProvider metadataProvider = null;

    public static boolean tryFlexibleJoin(AbstractBinaryJoinOperator joinOp, IOptimizationContext context,
            ILogicalExpression joinCondition, int left, int right) throws AlgebricksException {

        if (joinCondition.getExpressionTag() != LogicalExpressionTag.FUNCTION_CALL) {
            return false;
        }

        AbstractFunctionCallExpression funcExpr = (AbstractFunctionCallExpression) joinCondition;
        AbstractFunctionCallExpression flexibleFuncExpr = null;
        List<Mutable<ILogicalExpression>> conditionExprs = new ArrayList<>();

        IExternalFunctionInfo functionInfo = null;
        IExternalFunctionInfo functionInfoT = null;

        if (funcExpr.getFunctionIdentifier().equals(BuiltinFunctions.AND)) {
            List<Mutable<ILogicalExpression>> inputExprs = funcExpr.getArguments();
            if (inputExprs.size() == 0) {
                return false;
            }

            boolean flexibleJoinExists = false;
            boolean firstFlexible = true;
            for (Mutable<ILogicalExpression> exp : inputExprs) {
                AbstractFunctionCallExpression funcCallExp = (AbstractFunctionCallExpression) exp.getValue();
                if (funcCallExp.getFunctionInfo() instanceof IExternalFunctionInfo)
                    functionInfoT = (IExternalFunctionInfo) funcCallExp.getFunctionInfo();

                if (functionInfoT != null
                        && functionInfoT.getKind().equals(AbstractFunctionCallExpression.FunctionKind.FJ_CALLER)
                        && firstFlexible) {
                    functionInfo = functionInfoT;
                    flexibleFuncExpr = funcCallExp;
                    flexibleJoinExists = true;
                    firstFlexible = false;
                } else {
                    conditionExprs.add(exp);
                }
            }
            if (!flexibleJoinExists) {
                return false;
            }
        } else if (funcExpr.getFunctionInfo() instanceof IExternalFunctionInfo) {
            functionInfo = (IExternalFunctionInfo) funcExpr.getFunctionInfo();
            if (functionInfo.getKind().equals(AbstractFunctionCallExpression.FunctionKind.FJ_CALLER))
                flexibleFuncExpr = funcExpr;
            else
                return false;
        } else {
            return false;
        }

        metadataProvider = (MetadataProvider) context.getMetadataProvider();

        List<Mutable<ILogicalExpression>> joinArgs = flexibleFuncExpr.getArguments();
        /*if (joinArgs.size() != 2) {
            return false;
        }*/

        ILogicalExpression flexibleJoinLeftArg = joinArgs.get(left).getValue();
        ILogicalExpression flexibleJoinRightArg = joinArgs.get(right).getValue();

        if (flexibleJoinLeftArg.getExpressionTag() != LogicalExpressionTag.VARIABLE
                || flexibleJoinRightArg.getExpressionTag() != LogicalExpressionTag.VARIABLE) {
            return false;
        }

        FlexibleJoinAnnotation flexibleJoinAnn = funcExpr.getAnnotation(FlexibleJoinAnnotation.class);

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

        List<Mutable<ILogicalExpression>> parametersT = null;
        List<IAObject> parameters = new ArrayList<>();
        //List<IAType> parameterTypes = functionInfo.getParameterTypes().subList(2, joinArgs.size());
        if (joinArgs.size() > 2) {
            parametersT = joinArgs.subList(2, joinArgs.size());
            for (Mutable<ILogicalExpression> p : parametersT) {
                parameters.add(ConstantExpressionUtil.getConstantIaObject(p.getValue(), null));
            }
        }

        String libraryName = functionInfo.getLibraryName();
        DataverseName dataverseName = functionInfo.getLibraryDataverseName();
        String functionName = functionInfo.getFunctionIdentifier().getName();

        //Add a dynamic workflow for the summary one
        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> leftSummarizer =
                createSummary(joinOp, context, leftInputOp, leftInputVar, 0, dataverseName, functionName, parameters);
        MutableObject<ILogicalOperator> leftGlobalAgg = leftSummarizer.first;
        List<LogicalVariable> leftGlobalAggResultVars = leftSummarizer.second;
        MutableObject<ILogicalOperator> leftExchToJoinOpRef = leftSummarizer.third;
        LogicalVariable leftSummary = leftGlobalAggResultVars.get(0);

        //Add a dynamic workflow for the summary two
        Triple<MutableObject<ILogicalOperator>, List<LogicalVariable>, MutableObject<ILogicalOperator>> rightSummarizer =
                createSummary(joinOp, context, rightInputOp, rightInputVar, 1, dataverseName, functionName, parameters);
        MutableObject<ILogicalOperator> rightGlobalAgg = rightSummarizer.first;
        List<LogicalVariable> rightGlobalAggResultVars = rightSummarizer.second;
        MutableObject<ILogicalOperator> rightExchToJoinOpRef = rightSummarizer.third;
        LogicalVariable rightSummary = rightGlobalAggResultVars.get(0);

        // Join the left and right summaries
        Mutable<ILogicalExpression> trueCondition =
                new MutableObject<>(new ConstantExpression(new AsterixConstantValue(ABoolean.TRUE)));
        InnerJoinOperator summaryJoinOp = new InnerJoinOperator(trueCondition, leftGlobalAgg, rightGlobalAgg);
        summaryJoinOp.setPhysicalOperator(new NestedLoopJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER,
                AbstractJoinPOperator.JoinPartitioningType.BROADCAST));
        //summaryJoinOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.UNPARTITIONED);
        summaryJoinOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(summaryJoinOp);

        summaryJoinOp.setSourceLocation(joinOp.getSourceLocation());
        MutableObject<ILogicalOperator> divideJoinRef = new MutableObject<>(summaryJoinOp);
        summaryJoinOp.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(summaryJoinOp);

        //Call the divide function to create the Configuration

        String divideFunctionName = functionName + "_fj_divide";
        FunctionSignature divideFunctionSignature = new FunctionSignature(dataverseName, divideFunctionName, 2);
        Function divideFunction = metadataProvider.lookupUserDefinedFunction(divideFunctionSignature);
        ExternalFJFunctionInfo divideFunctionInfo =
                ExternalFunctionCompilerUtil.getFJFunctionInfo(metadataProvider, divideFunction, parameters);

        List<Mutable<ILogicalExpression>> getDivideFuncInputExprs = new ArrayList<>();
        getDivideFuncInputExprs.add(new MutableObject<>(new VariableReferenceExpression(leftSummary)));
        getDivideFuncInputExprs.add(new MutableObject<>(new VariableReferenceExpression(rightSummary)));
        ScalarFunctionCallExpression getDivideFuncExpr =
                new ScalarFunctionCallExpression(divideFunctionInfo, getDivideFuncInputExprs);
        getDivideFuncExpr.setSourceLocation(joinOp.getSourceLocation());

        Mutable<ILogicalExpression> configurationExpr = new MutableObject<>(getDivideFuncExpr);
        LogicalVariable configuration = context.newVar();
        AbstractLogicalOperator configurationAssignOperator = new AssignOperator(configuration, configurationExpr);
        //configurationAssignOperator.
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

        //configReplicateOperator.getOutputMaterializationFlags()[0] = true;

        // Replicate configuration to the left branch
        ExchangeOperator exchConfigToJoinOpLeft =
                createBroadcastExchangeOp(configReplicateOperator, context, joinOp.getSourceLocation());
        MutableObject<ILogicalOperator> exchConfigToJoinOpLeftRef = new MutableObject<>(exchConfigToJoinOpLeft);

        Pair<LogicalVariable, Mutable<ILogicalOperator>> createLeftAssignProjectOperatorResult =
                createAssignProjectOperator(joinOp, configuration, configReplicateOperator, exchConfigToJoinOpLeftRef,
                        context);
        LogicalVariable leftConfigurationVar = createLeftAssignProjectOperatorResult.getFirst();
        Mutable<ILogicalOperator> leftConfigurationRef = createLeftAssignProjectOperatorResult.getSecond();

        // Replicate configuration to the right branch
        ExchangeOperator exchConfigToJoinOpRight =
                createBroadcastExchangeOp(configReplicateOperator, context, joinOp.getSourceLocation());
        MutableObject<ILogicalOperator> exchConfigToJoinOpRightRef = new MutableObject<>(exchConfigToJoinOpRight);
        Pair<LogicalVariable, Mutable<ILogicalOperator>> createRightAssignProjectOperatorResult =
                createAssignProjectOperator(joinOp, configuration, configReplicateOperator, exchConfigToJoinOpRightRef,
                        context);
        LogicalVariable rightConfigurationVar = createRightAssignProjectOperatorResult.getFirst();
        Mutable<ILogicalOperator> rightConfigurationRef = createRightAssignProjectOperatorResult.getSecond();

        //Replicate Configuration to the right branch of a later Nested Loop Join for the verify function
        ExchangeOperator exchConfigToVerifyJoinOp =
                createBroadcastExchangeOp(configReplicateOperator, context, joinOp.getSourceLocation());
        MutableObject<ILogicalOperator> exchConfigToVerifyJoinOpRef = new MutableObject<>(exchConfigToVerifyJoinOp);

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
        Triple<LogicalVariable, UnnestOperator, UnnestingFunctionCallExpression> leftBucketIdVarPair =
                ApplyFlexibleJoinUtils.injectAssignOneUnnestOperator(context, leftInputOp, leftInputVar,
                        leftConfigurationExpr, dataverseName, functionName, parameters);
        Triple<LogicalVariable, UnnestOperator, UnnestingFunctionCallExpression> rightBucketIdVarPair =
                ApplyFlexibleJoinUtils.injectAssignTwoUnnestOperator(context, rightInputOp, rightInputVar,
                        rightConfigurationExpr, dataverseName, functionName, parameters);

        LogicalVariable leftBucketIdVar = leftBucketIdVarPair.first;
        LogicalVariable rightBucketIdVar = rightBucketIdVarPair.first;

        ScalarFunctionCallExpression verifyJoinCondition =
                createVerifyCondition(joinOp, verifyConfigurationExpr, leftBucketIdVar, rightBucketIdVar, leftInputVar,
                        rightInputVar, dataverseName, functionName, parameters);

        List<LogicalVariable> keysLeftBranch = new ArrayList<>();
        keysLeftBranch.add(leftBucketIdVar);
        keysLeftBranch.add(leftInputVar);

        List<LogicalVariable> keysRightBranch = new ArrayList<>();
        keysRightBranch.add(rightBucketIdVar);
        keysRightBranch.add(rightInputVar);

        String matchFunctionName = functionName + "_fj_match";
        FunctionSignature matchFunctionSignature = new FunctionSignature(dataverseName, matchFunctionName, 2);
        Function matchFunction = metadataProvider.lookupUserDefinedFunction(matchFunctionSignature);
        FunctionInfo MatchFunctionInfo;

        boolean eqMatch = false;
        if (matchFunction == null) {
            MatchFunctionInfo = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.EQ);
            eqMatch = true;
        } else {
            MatchFunctionInfo =
                    ExternalFunctionCompilerUtil.getFJFunctionInfo(metadataProvider, matchFunction, parameters);
        }

        //MatchFunctionInfo = BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.EQ);

        ScalarFunctionCallExpression match = new ScalarFunctionCallExpression(MatchFunctionInfo,
                new MutableObject<>(new VariableReferenceExpression(leftBucketIdVar)),
                new MutableObject<>(new VariableReferenceExpression(rightBucketIdVar)));

        conditionExprs.add(new MutableObject<>(match));

        ScalarFunctionCallExpression updatedJoinCondition;
        if (conditionExprs.size() > 1) {
            updatedJoinCondition = new ScalarFunctionCallExpression(
                    BuiltinFunctions.getBuiltinFunctionInfo(BuiltinFunctions.AND), conditionExprs);
            updatedJoinCondition.setSourceLocation(joinOp.getSourceLocation());
        } else {
            updatedJoinCondition = match;
        }
        //Mutable<ILogicalExpression> joinConditionRef = joinOp.getCondition();
        //joinConditionRef.setValue(updatedJoinCondition);

        InnerJoinOperator matchJoinOp = new InnerJoinOperator(new MutableObject<>(updatedJoinCondition),
                new MutableObject<>(leftBucketIdVarPair.second), new MutableObject<>(rightBucketIdVarPair.second));
        //matchJoinOp.setPhysicalOperator(new HybridHashJoinPOperator(AbstractBinaryJoinOperator.JoinKind.INNER, AbstractJoinPOperator.JoinPartitioningType.PAIRWISE,
        //        keysLeftBranch, keysRightBranch, ));

        if (flexibleJoinAnn == null && !eqMatch) {
            setFlexibleJoinOp(matchJoinOp, keysLeftBranch, keysRightBranch, context);
        }

        matchJoinOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        matchJoinOp.setSourceLocation(joinOp.getSourceLocation());
        matchJoinOp.setSchema(joinOp.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(matchJoinOp);

        Mutable<ILogicalOperator> opRef = new MutableObject<>(joinOp);
        Mutable<ILogicalOperator> flexibleJoinOpRef = new MutableObject<>(matchJoinOp);

        InnerJoinOperator verifyJoinOp = new InnerJoinOperator(new MutableObject<>(verifyJoinCondition),
                flexibleJoinOpRef, exchConfigToVerifyJoinOpRef);
        MutableObject<ILogicalOperator> verifyJoinOpRef = new MutableObject<>(verifyJoinOp);
        verifyJoinOp.setSourceLocation(joinOp.getSourceLocation());
        context.computeAndSetTypeEnvironmentForOperator(verifyJoinOp);
        verifyJoinOp.recomputeSchema();
        opRef.setValue(verifyJoinOpRef.getValue());
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
            MutableObject<ILogicalOperator> exchToLocalAggRef, int one, DataverseName dataverseName,
            String callerFunction, List<IAObject> parameters) throws AlgebricksException {

        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        List<Mutable<ILogicalExpression>> fields = new ArrayList<>(1);
        fields.add(new MutableObject<>(inputVarRef));

        ExternalFJFunctionInfo localAggFunc;
        FunctionSignature functionSignature;
        String functionName = callerFunction + "_fj_local_summary_two";
        functionSignature = new FunctionSignature(dataverseName, functionName, 1);
        Function function = metadataProvider.lookupUserDefinedFunction(functionSignature);
        if (function == null || one == 0) {
            functionName = callerFunction + "_fj_local_summary_one";
            functionSignature = new FunctionSignature(dataverseName, functionName, 1);
            function = metadataProvider.lookupUserDefinedFunction(functionSignature);
        }

        localAggFunc = ExternalFunctionCompilerUtil.getFJFunctionInfo(metadataProvider, function, parameters);

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
        return createGlobalAggregateOperator(op, context, localOutVariable, localAgg, one, dataverseName,
                callerFunction, parameters);
    }

    private static Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createGlobalAggregateOperator(
            AbstractBinaryJoinOperator op, IOptimizationContext context, LogicalVariable inputVar,
            MutableObject<ILogicalOperator> inputOperator, int one, DataverseName dataverseName, String callerFunction,
            List<IAObject> parameters) throws AlgebricksException {

        List<Mutable<ILogicalExpression>> globalAggFuncArgs = new ArrayList<>(1);
        AbstractLogicalExpression inputVarRef = new VariableReferenceExpression(inputVar, op.getSourceLocation());
        globalAggFuncArgs.add(new MutableObject<>(inputVarRef));

        ExternalFJFunctionInfo globalAggFunc;
        FunctionSignature functionSignature;
        String functionName = callerFunction + "_fj_global_summary_two";
        functionSignature = new FunctionSignature(dataverseName, functionName, 1);
        Function function = metadataProvider.lookupUserDefinedFunction(functionSignature);
        if (function == null || one == 0) {
            functionName = callerFunction + "_fj_global_summary_one";
            functionSignature = new FunctionSignature(dataverseName, functionName, 1);
            function = metadataProvider.lookupUserDefinedFunction(functionSignature);
        }
        globalAggFunc = (ExternalFJFunctionInfo) ExternalFunctionCompilerUtil.getFJFunctionInfo(metadataProvider,
                function, parameters);

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
            LogicalVariable inputVar, int one, DataverseName dataverseName, String callerFunction,
            List<IAObject> parameters) throws AlgebricksException {
        // Add ReplicationOperator for the input branch
        SourceLocation sourceLocation = op.getSourceLocation();
        ReplicateOperator replicateOperator = createReplicateOperator(inputOp, context, sourceLocation, 2);

        // Create one to one exchange operators for the replicator of the input branch
        ExchangeOperator exchToForward = createOneToOneExchangeOp(replicateOperator, context, sourceLocation);
        MutableObject<ILogicalOperator> exchToForwardRef = new MutableObject<>(exchToForward);

        ExchangeOperator exchToLocalAgg = createOneToOneExchangeOp(replicateOperator, context, op.getSourceLocation());
        MutableObject<ILogicalOperator> exchToLocalAggRef = new MutableObject<>(exchToLocalAgg);

        // Materialize the data to be able to re-read the data again
        replicateOperator.getOutputMaterializationFlags()[0] = true;

        Pair<MutableObject<ILogicalOperator>, List<LogicalVariable>> createLocalAndGlobalAggResult =
                createLocalAndGlobalAggregateOperators(op, context, inputVar, exchToLocalAggRef, one, dataverseName,
                        callerFunction, parameters);
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

    private static Triple<LogicalVariable, UnnestOperator, UnnestingFunctionCallExpression> injectAssignOneUnnestOperator(
            IOptimizationContext context, Mutable<ILogicalOperator> op, LogicalVariable unnestVar,
            Mutable<ILogicalExpression> configureExpr, DataverseName dataverseName, String functionCall,
            List<IAObject> parameters) throws AlgebricksException {
        SourceLocation srcLoc = op.getValue().getSourceLocation();
        LogicalVariable bucketIdVar = context.newVar();
        VariableReferenceExpression unnestVarRef = new VariableReferenceExpression(unnestVar);
        unnestVarRef.setSourceLocation(srcLoc);

        String verifyFunctionName = functionCall + "_fj_assign_one";
        FunctionSignature functionSignature = new FunctionSignature(dataverseName, verifyFunctionName, 2);
        Function function = metadataProvider.lookupUserDefinedFunction(functionSignature);
        ExternalFJFunctionInfo externalFunctionInfo =
                ExternalFunctionCompilerUtil.getFJFunctionInfo(metadataProvider, function, parameters);

        UnnestingFunctionCallExpression assignFuncExpr = new UnnestingFunctionCallExpression(externalFunctionInfo,
                new MutableObject<>(unnestVarRef), configureExpr);
        assignFuncExpr.setSourceLocation(srcLoc);
        UnnestOperator unnestOp = new UnnestOperator(bucketIdVar, new MutableObject<>(assignFuncExpr));
        unnestOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);
        unnestOp.setPhysicalOperator(new UnnestPOperator());
        unnestOp.setSourceLocation(srcLoc);
        unnestOp.getInputs().add(new MutableObject<>(op.getValue()));
        context.computeAndSetTypeEnvironmentForOperator(unnestOp);
        unnestOp.recomputeSchema();
        op.setValue(unnestOp);
        return new Triple<>(bucketIdVar, unnestOp, assignFuncExpr);
    }

    private static Triple<LogicalVariable, UnnestOperator, UnnestingFunctionCallExpression> injectAssignTwoUnnestOperator(
            IOptimizationContext context, Mutable<ILogicalOperator> op, LogicalVariable unnestVar,
            Mutable<ILogicalExpression> configureExpr, DataverseName dataverseName, String functionCall,
            List<IAObject> parameters) throws AlgebricksException {
        SourceLocation srcLoc = op.getValue().getSourceLocation();
        LogicalVariable bucketIdVar = context.newVar();
        VariableReferenceExpression unnestVarRef = new VariableReferenceExpression(unnestVar);
        unnestVarRef.setSourceLocation(srcLoc);

        String functionName = functionCall + "_fj_assign_two";
        FunctionSignature functionSignature = new FunctionSignature(dataverseName, functionName, 2);
        Function function = metadataProvider.lookupUserDefinedFunction(functionSignature);
        if (function == null) {
            functionName = functionCall + "_fj_assign_one";
            functionSignature = new FunctionSignature(dataverseName, functionName, 2);
            function = metadataProvider.lookupUserDefinedFunction(functionSignature);
        }
        ExternalFJFunctionInfo externalFunctionInfo =
                ExternalFunctionCompilerUtil.getFJFunctionInfo(metadataProvider, function, parameters);
        UnnestingFunctionCallExpression spatialTileFuncExpr = new UnnestingFunctionCallExpression(externalFunctionInfo,
                new MutableObject<>(unnestVarRef), configureExpr);
        spatialTileFuncExpr.setSourceLocation(srcLoc);
        UnnestOperator unnestOp = new UnnestOperator(bucketIdVar, new MutableObject<>(spatialTileFuncExpr));
        unnestOp.setExecutionMode(AbstractLogicalOperator.ExecutionMode.LOCAL);

        unnestOp.setPhysicalOperator(new UnnestPOperator());
        unnestOp.setSourceLocation(srcLoc);
        unnestOp.getInputs().add(new MutableObject<>(op.getValue()));
        context.computeAndSetTypeEnvironmentForOperator(unnestOp);
        unnestOp.recomputeSchema();
        op.setValue(unnestOp);
        return new Triple<>(bucketIdVar, unnestOp, spatialTileFuncExpr);
    }

    private static ExchangeOperator createBroadcastExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setSourceLocation(sourceLocation);
        exchangeOperator.setPhysicalOperator(new BroadcastExchangePOperator(context.getComputationNodeDomain()));
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        //exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
        exchangeOperator.setSchema(replicateOperator.getSchema());
        context.computeAndSetTypeEnvironmentForOperator(exchangeOperator);
        return exchangeOperator;
    }

    private static ExchangeOperator createHashExchangeOp(ReplicateOperator replicateOperator,
            IOptimizationContext context, SourceLocation sourceLocation) throws AlgebricksException {
        ExchangeOperator exchangeOperator = new ExchangeOperator();
        exchangeOperator.setSourceLocation(sourceLocation);
        exchangeOperator.setPhysicalOperator(new BroadcastExchangePOperator(context.getComputationNodeDomain()));
        replicateOperator.getOutputs().add(new MutableObject<>(exchangeOperator));
        exchangeOperator.getInputs().add(new MutableObject<>(replicateOperator));
        //exchangeOperator.setExecutionMode(AbstractLogicalOperator.ExecutionMode.PARTITIONED);
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
        projectOperator.getInputs().add(new MutableObject<>(assignOperator));

        context.computeAndSetTypeEnvironmentForOperator(assignOperator);
        assignOperator.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(projectOperator);
        projectOperator.recomputeSchema();
        Mutable<ILogicalOperator> projectOperatorRef = new MutableObject<>(projectOperator);

        return new Pair<>(newFinalMbrVar, projectOperatorRef);
    }

    private static ScalarFunctionCallExpression createVerifyCondition(AbstractBinaryJoinOperator op,
            Mutable<ILogicalExpression> configurationExpr, LogicalVariable leftBucketIdVar,
            LogicalVariable rightBucketIdVar, LogicalVariable leftInputVar, LogicalVariable rightInputVar,
            DataverseName dataverseName, String functionName, List<IAObject> parameters) throws AlgebricksException {

        String verifyFunctionName = functionName + "_fj_verify";
        FunctionSignature verifyFunctionSignature = new FunctionSignature(dataverseName, verifyFunctionName, 5);
        Function verifyFunction = metadataProvider.lookupUserDefinedFunction(verifyFunctionSignature);
        ExternalFJFunctionInfo verifyFunctionInfo =
                ExternalFunctionCompilerUtil.getFJFunctionInfo(metadataProvider, verifyFunction, parameters);
        ScalarFunctionCallExpression verify = new ScalarFunctionCallExpression(verifyFunctionInfo,
                new MutableObject<>(new VariableReferenceExpression(leftBucketIdVar)),
                new MutableObject<>(new VariableReferenceExpression(leftInputVar)),
                new MutableObject<>(new VariableReferenceExpression(rightBucketIdVar)),
                new MutableObject<>(new VariableReferenceExpression(rightInputVar)), configurationExpr

        );
        verify.setSourceLocation(op.getSourceLocation());
        return verify;
    }

    private static void setFlexibleJoinOp(AbstractBinaryJoinOperator op, List<LogicalVariable> keysLeftBranch,
            List<LogicalVariable> keysRightBranch, IOptimizationContext context) throws AlgebricksException {
        op.setPhysicalOperator(
                new FlexibleJoinPOperator(op.getJoinKind(), AbstractJoinPOperator.JoinPartitioningType.PAIRWISE,
                        keysLeftBranch, keysRightBranch, context.getPhysicalOptimizationConfig().getMaxFramesForJoin(),
                        context.getPhysicalOptimizationConfig().getFudgeFactor()));
        op.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(op);
    }

    private static void setHashJoinOp(AbstractBinaryJoinOperator op, List<LogicalVariable> sideLeft,
            List<LogicalVariable> sideRight, IOptimizationContext context) throws AlgebricksException {
        op.setPhysicalOperator(
                new HybridHashJoinPOperator(op.getJoinKind(), AbstractJoinPOperator.JoinPartitioningType.BROADCAST,
                        sideLeft, sideRight, context.getPhysicalOptimizationConfig().getMaxFramesForJoinLeftInput(),
                        context.getPhysicalOptimizationConfig().getMaxRecordsPerFrame(),
                        context.getPhysicalOptimizationConfig().getFudgeFactor()));
        op.recomputeSchema();
        context.computeAndSetTypeEnvironmentForOperator(op);
    }
}
