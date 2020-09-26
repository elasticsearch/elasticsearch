/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.WriterConstants;
import org.elasticsearch.painless.ir.BinaryImplNode;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.BooleanNode;
import org.elasticsearch.painless.ir.BreakNode;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ConditionalNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ContinueNode;
import org.elasticsearch.painless.ir.DeclarationBlockNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.DefInterfaceReferenceNode;
import org.elasticsearch.painless.ir.DoWhileLoopNode;
import org.elasticsearch.painless.ir.DupNode;
import org.elasticsearch.painless.ir.ElvisNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.ir.FlipArrayIndexNode;
import org.elasticsearch.painless.ir.FlipCollectionIndexNode;
import org.elasticsearch.painless.ir.FlipDefIndexNode;
import org.elasticsearch.painless.ir.ForEachLoopNode;
import org.elasticsearch.painless.ir.ForEachSubArrayNode;
import org.elasticsearch.painless.ir.ForEachSubIterableNode;
import org.elasticsearch.painless.ir.ForLoopNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.IRNode;
import org.elasticsearch.painless.ir.IfElseNode;
import org.elasticsearch.painless.ir.IfNode;
import org.elasticsearch.painless.ir.InstanceofNode;
import org.elasticsearch.painless.ir.InvokeCallDefNode;
import org.elasticsearch.painless.ir.InvokeCallMemberNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.ListInitializationNode;
import org.elasticsearch.painless.ir.LoadBraceDefNode;
import org.elasticsearch.painless.ir.LoadBraceNode;
import org.elasticsearch.painless.ir.LoadDotArrayLengthNode;
import org.elasticsearch.painless.ir.LoadDotDefNode;
import org.elasticsearch.painless.ir.LoadDotNode;
import org.elasticsearch.painless.ir.LoadDotShortcutNode;
import org.elasticsearch.painless.ir.LoadFieldMemberNode;
import org.elasticsearch.painless.ir.LoadListShortcutNode;
import org.elasticsearch.painless.ir.LoadMapShortcutNode;
import org.elasticsearch.painless.ir.LoadVariableNode;
import org.elasticsearch.painless.ir.MapInitializationNode;
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.ir.NewObjectNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.NullSafeSubNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.StoreBraceDefNode;
import org.elasticsearch.painless.ir.StoreBraceNode;
import org.elasticsearch.painless.ir.StoreDotDefNode;
import org.elasticsearch.painless.ir.StoreDotNode;
import org.elasticsearch.painless.ir.StoreDotShortcutNode;
import org.elasticsearch.painless.ir.StoreFieldMemberNode;
import org.elasticsearch.painless.ir.StoreListShortcutNode;
import org.elasticsearch.painless.ir.StoreMapShortcutNode;
import org.elasticsearch.painless.ir.StoreVariableNode;
import org.elasticsearch.painless.ir.StringConcatenationNode;
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.ir.TryNode;
import org.elasticsearch.painless.ir.TypedCaptureReferenceNode;
import org.elasticsearch.painless.ir.TypedInterfaceReferenceNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.ir.WhileLoopNode;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExpressionType;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.WriteScope;
import org.elasticsearch.painless.symbol.WriteScope.Variable;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.util.Printer;

import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.painless.WriterConstants.BASE_INTERFACE_TYPE;
import static org.elasticsearch.painless.WriterConstants.CLASS_TYPE;
import static org.elasticsearch.painless.WriterConstants.EQUALS;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_HASNEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_NEXT;
import static org.elasticsearch.painless.WriterConstants.ITERATOR_TYPE;
import static org.elasticsearch.painless.WriterConstants.OBJECTS_TYPE;

public class DefaultIRTreeToASMBytesPhase implements IRTreeVisitor<WriteScope> {

    protected void visit(IRNode irNode, WriteScope writeScope) {
        irNode.visit(this, writeScope);
    }

    public void visitScript(ClassNode irClassNode) {
        WriteScope writeScope = WriteScope.newScriptScope();
        visitClass(irClassNode, writeScope);
    }

    @Override
    public void visitClass(ClassNode irClassNode, WriteScope writeScope) {
        ScriptScope scriptScope = irClassNode.getScriptScope();
        ScriptClassInfo scriptClassInfo = scriptScope.getScriptClassInfo();
        BitSet statements = new BitSet(scriptScope.getScriptSource().length());
        scriptScope.addStaticConstant("$STATEMENTS", statements);
        Printer debugStream = irClassNode.getDebugStream();

        // Create the ClassWriter.

        int classFrames = org.objectweb.asm.ClassWriter.COMPUTE_FRAMES | org.objectweb.asm.ClassWriter.COMPUTE_MAXS;
        int classAccess = Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL;
        String interfaceBase = BASE_INTERFACE_TYPE.getInternalName();
        String className = CLASS_TYPE.getInternalName();
        String[] classInterfaces = new String[] { interfaceBase };

        ClassWriter classWriter = new ClassWriter(scriptScope.getCompilerSettings(), statements, debugStream,
                scriptClassInfo.getBaseClass(), classFrames, classAccess, className, classInterfaces);
        ClassVisitor classVisitor = classWriter.getClassVisitor();
        classVisitor.visitSource(Location.computeSourceName(scriptScope.getScriptName()), null);
        writeScope = writeScope.newClassScope(classWriter);

        org.objectweb.asm.commons.Method init;

        if (scriptClassInfo.getBaseClass().getConstructors().length == 0) {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class).toMethodDescriptorString());
        } else {
            init = new org.objectweb.asm.commons.Method("<init>", MethodType.methodType(void.class,
                    scriptClassInfo.getBaseClass().getConstructors()[0].getParameterTypes()).toMethodDescriptorString());
        }

        // Write the constructor:
        MethodWriter constructor = classWriter.newMethodWriter(Opcodes.ACC_PUBLIC, init);
        constructor.visitCode();
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(Type.getType(scriptClassInfo.getBaseClass()), init);
        constructor.returnValue();
        constructor.endMethod();

        BlockNode irClinitBlockNode = irClassNode.getClinitBlockNode();

        if (irClinitBlockNode.getStatementsNodes().isEmpty() == false) {
            MethodWriter methodWriter = classWriter.newMethodWriter(
                    Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC,
                    new Method("<clinit>", Type.getType(void.class), new Type[0]));
            visit(irClinitBlockNode, writeScope.newMethodScope(methodWriter).newBlockScope());
            methodWriter.returnValue();
            methodWriter.endMethod();
        }

        // Write all fields:
        for (FieldNode irFieldNode : irClassNode.getFieldsNodes()) {
            visit(irFieldNode, writeScope);
        }

        // Write all functions:
        for (FunctionNode irFunctionNode : irClassNode.getFunctionsNodes()) {
            visit(irFunctionNode, writeScope);
        }

        // End writing the class and store the generated bytes.
        classVisitor.visitEnd();
        irClassNode.setBytes(classWriter.getClassBytes());
    }

    @Override
    public void visitFunction(FunctionNode irFunctionNode, WriteScope writeScope) {
        int access = Opcodes.ACC_PUBLIC;

        if (irFunctionNode.isStatic()) {
            access |= Opcodes.ACC_STATIC;
        }

        if (irFunctionNode.hasVarArgs()) {
            access |= Opcodes.ACC_VARARGS;
        }

        if (irFunctionNode.isSynthetic()) {
            access |= Opcodes.ACC_SYNTHETIC;
        }

        Type asmReturnType = MethodWriter.getType(irFunctionNode.getReturnType());
        List<Class<?>> typeParameters = irFunctionNode.getTypeParameters();
        Type[] asmParameterTypes = new Type[typeParameters.size()];

        for (int index = 0; index < asmParameterTypes.length; ++index) {
            asmParameterTypes[index] = MethodWriter.getType(typeParameters.get(index));
        }

        Method method = new Method(irFunctionNode.getName(), asmReturnType, asmParameterTypes);

        ClassWriter classWriter = writeScope.getClassWriter();
        MethodWriter methodWriter = classWriter.newMethodWriter(access, method);
        writeScope = writeScope.newMethodScope(methodWriter);

        if (irFunctionNode.isStatic() == false) {
            writeScope.defineInternalVariable(Object.class, "this");
        }

        List<String> parameterNames = irFunctionNode.getParameterNames();

        for (int index = 0; index < typeParameters.size(); ++index) {
            writeScope.defineVariable(typeParameters.get(index), parameterNames.get(index));
        }

        methodWriter.visitCode();

        if (irFunctionNode.getMaxLoopCounter() > 0) {
            // if there is infinite loop protection, we do this once:
            // int #loop = settings.getMaxLoopCounter()

            Variable loop = writeScope.defineInternalVariable(int.class, "loop");

            methodWriter.push(irFunctionNode.getMaxLoopCounter());
            methodWriter.visitVarInsn(Opcodes.ISTORE, loop.getSlot());
        }

        visit(irFunctionNode.getBlockNode(), writeScope.newBlockScope());

        methodWriter.endMethod();
    }

    @Override
    public void visitField(FieldNode irFieldNode, WriteScope writeScope) {
        ClassWriter classWriter = writeScope.getClassWriter();
        classWriter.getClassVisitor().visitField(
                ClassWriter.buildAccess(irFieldNode.getModifiers(), true), irFieldNode.getName(),
                Type.getType(irFieldNode.getFieldType()).getDescriptor(), null, null).visitEnd();
    }

    @Override
    public void visitBlock(BlockNode irBlockNode, WriteScope writeScope) {
        for (StatementNode statementNode : irBlockNode.getStatementsNodes()) {
            visit(statementNode, writeScope);
        }
    }

    @Override
    public void visitIf(IfNode irIfNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irIfNode.getLocation());

        Label fals = new Label();

        visit(irIfNode.getConditionNode(), writeScope);
        methodWriter.ifZCmp(Opcodes.IFEQ, fals);
        visit(irIfNode.getBlockNode(), writeScope.newBlockScope());
        methodWriter.mark(fals);
    }

    @Override
    public void visitIfElse(IfElseNode irIfElseNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irIfElseNode.getLocation());

        Label fals = new Label();
        Label end = new Label();

        visit(irIfElseNode.getConditionNode(), writeScope);
        methodWriter.ifZCmp(Opcodes.IFEQ, fals);
        visit(irIfElseNode.getBlockNode(), writeScope.newBlockScope());

        if (irIfElseNode.getBlockNode().doAllEscape() == false) {
            methodWriter.goTo(end);
        }

        methodWriter.mark(fals);
        visit(irIfElseNode.getElseBlockNode(), writeScope.newBlockScope());
        methodWriter.mark(end);
    }

    @Override
    public void visitWhileLoop(WhileLoopNode irWhileLoopNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irWhileLoopNode.getLocation());

        writeScope = writeScope.newBlockScope();

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        if (irWhileLoopNode.isContinuous() == false) {
            visit(irWhileLoopNode.getConditionNode(), writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), irWhileLoopNode.getLocation());
        }

        BlockNode irBlockNode = irWhileLoopNode.getBlockNode();

        if (irBlockNode != null) {
            visit(irBlockNode, writeScope.newLoopScope(begin, end));
        }

        if (irBlockNode == null || irBlockNode.doAllEscape() == false) {
            methodWriter.goTo(begin);
        }

        methodWriter.mark(end);
    }

    @Override
    public void visitDoWhileLoop(DoWhileLoopNode irDoWhileLoopNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irDoWhileLoopNode.getLocation());

        writeScope = writeScope.newBlockScope();

        Label start = new Label();
        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(start);
        visit(irDoWhileLoopNode.getBlockNode(), writeScope.newLoopScope(begin, end));
        methodWriter.mark(begin);

        if (irDoWhileLoopNode.isContinuous() == false) {
            visit(irDoWhileLoopNode.getConditionNode(), writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), irDoWhileLoopNode.getLocation());
        }

        methodWriter.goTo(start);
        methodWriter.mark(end);
    }

    @Override
    public void visitForLoop(ForLoopNode irForLoopNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irForLoopNode.getLocation());

        IRNode irInitializerNode = irForLoopNode.getInitializerNode();
        ExpressionNode irConditionNode = irForLoopNode.getConditionNode();
        ExpressionNode irAfterthoughtNode = irForLoopNode.getAfterthoughtNode();
        BlockNode irBlockNode = irForLoopNode.getBlockNode();

        writeScope = writeScope.newBlockScope();

        Label start = new Label();
        Label begin = irAfterthoughtNode == null ? start : new Label();
        Label end = new Label();

        if (irInitializerNode instanceof DeclarationBlockNode) {
            visit(irInitializerNode, writeScope);
        } else if (irInitializerNode instanceof ExpressionNode) {
            ExpressionNode irExpressionNode = (ExpressionNode)irInitializerNode;

            visit(irExpressionNode, writeScope);
            methodWriter.writePop(MethodWriter.getType(irExpressionNode.getDecoration(IRDExpressionType.class).getType()).getSize());
        }

        methodWriter.mark(start);

        if (irConditionNode != null && irForLoopNode.isContinuous() == false) {
            visit(irConditionNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, end);
        }

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), irForLoopNode.getLocation());
        }

        boolean allEscape = false;

        if (irBlockNode != null) {
            allEscape = irBlockNode.doAllEscape();
            visit(irBlockNode, writeScope.newLoopScope(begin, end));
        }

        if (irAfterthoughtNode != null) {
            methodWriter.mark(begin);
            visit(irAfterthoughtNode, writeScope);
            methodWriter.writePop(MethodWriter.getType(irAfterthoughtNode.getDecoration(IRDExpressionType.class).getType()).getSize());
        }

        if (irAfterthoughtNode != null || allEscape == false) {
            methodWriter.goTo(start);
        }

        methodWriter.mark(end);
    }

    @Override
    public void visitForEachLoop(ForEachLoopNode irForEachLoopNode, WriteScope writeScope) {
        visit(irForEachLoopNode.getConditionNode(), writeScope.newBlockScope());
    }

    @Override
    public void visitForEachSubArrayLoop(ForEachSubArrayNode irForEachSubArrayNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irForEachSubArrayNode.getLocation());

        Variable variable = writeScope.defineVariable(irForEachSubArrayNode.getVariableType(), irForEachSubArrayNode.getVariableName());
        Variable array = writeScope.defineInternalVariable(irForEachSubArrayNode.getArrayType(), irForEachSubArrayNode.getArrayName());
        Variable index = writeScope.defineInternalVariable(irForEachSubArrayNode.getIndexType(), irForEachSubArrayNode.getIndexName());

        visit(irForEachSubArrayNode.getConditionNode(), writeScope);
        methodWriter.visitVarInsn(array.getAsmType().getOpcode(Opcodes.ISTORE), array.getSlot());
        methodWriter.push(-1);
        methodWriter.visitVarInsn(index.getAsmType().getOpcode(Opcodes.ISTORE), index.getSlot());

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        methodWriter.visitIincInsn(index.getSlot(), 1);
        methodWriter.visitVarInsn(index.getAsmType().getOpcode(Opcodes.ILOAD), index.getSlot());
        methodWriter.visitVarInsn(array.getAsmType().getOpcode(Opcodes.ILOAD), array.getSlot());
        methodWriter.arrayLength();
        methodWriter.ifICmp(MethodWriter.GE, end);

        methodWriter.visitVarInsn(array.getAsmType().getOpcode(Opcodes.ILOAD), array.getSlot());
        methodWriter.visitVarInsn(index.getAsmType().getOpcode(Opcodes.ILOAD), index.getSlot());
        methodWriter.arrayLoad(MethodWriter.getType(irForEachSubArrayNode.getIndexedType()));
        methodWriter.writeCast(irForEachSubArrayNode.getCast());
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), irForEachSubArrayNode.getLocation());
        }

        visit(irForEachSubArrayNode.getBlockNode(), writeScope.newLoopScope(begin, end));

        methodWriter.goTo(begin);
        methodWriter.mark(end);
    }

    @Override
    public void visitForEachSubIterableLoop(ForEachSubIterableNode irForEachSubIterableNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irForEachSubIterableNode.getLocation());

        Variable variable = writeScope.defineVariable(
                irForEachSubIterableNode.getVariableType(), irForEachSubIterableNode.getVariableName());
        Variable iterator = writeScope.defineInternalVariable(
                irForEachSubIterableNode.getIteratorType(), irForEachSubIterableNode.getIteratorName());

        visit(irForEachSubIterableNode.getConditionNode(), writeScope);

        if (irForEachSubIterableNode.getMethod() == null) {
            org.objectweb.asm.Type methodType = org.objectweb.asm.Type
                    .getMethodType(org.objectweb.asm.Type.getType(Iterator.class), org.objectweb.asm.Type.getType(Object.class));
            methodWriter.invokeDefCall("iterator", methodType, DefBootstrap.ITERATOR);
        } else {
            methodWriter.invokeMethodCall(irForEachSubIterableNode.getMethod());
        }

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ISTORE), iterator.getSlot());

        Label begin = new Label();
        Label end = new Label();

        methodWriter.mark(begin);

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ILOAD), iterator.getSlot());
        methodWriter.invokeInterface(ITERATOR_TYPE, ITERATOR_HASNEXT);
        methodWriter.ifZCmp(MethodWriter.EQ, end);

        methodWriter.visitVarInsn(iterator.getAsmType().getOpcode(Opcodes.ILOAD), iterator.getSlot());
        methodWriter.invokeInterface(ITERATOR_TYPE, ITERATOR_NEXT);
        methodWriter.writeCast(irForEachSubIterableNode.getCast());
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());

        Variable loop = writeScope.getInternalVariable("loop");

        if (loop != null) {
            methodWriter.writeLoopCounter(loop.getSlot(), irForEachSubIterableNode.getLocation());
        }

        visit(irForEachSubIterableNode.getBlockNode(), writeScope.newLoopScope(begin, end));
        methodWriter.goTo(begin);
        methodWriter.mark(end);
    }

    @Override
    public void visitDeclarationBlock(DeclarationBlockNode irDeclarationBlockNode, WriteScope writeScope) {
        for (DeclarationNode declarationNode : irDeclarationBlockNode.getDeclarationsNodes()) {
            visit(declarationNode, writeScope);
        }
    }

    @Override
    public void visitDeclaration(DeclarationNode irDeclarationNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irDeclarationNode.getLocation());

        Variable variable = writeScope.defineVariable(irDeclarationNode.getDeclarationType(), irDeclarationNode.getName());

        if (irDeclarationNode.getExpressionNode() == null) {
            Class<?> sort = variable.getType();

            if (sort == void.class || sort == boolean.class || sort == byte.class ||
                    sort == short.class || sort == char.class || sort == int.class) {
                methodWriter.push(0);
            } else if (sort == long.class) {
                methodWriter.push(0L);
            } else if (sort == float.class) {
                methodWriter.push(0F);
            } else if (sort == double.class) {
                methodWriter.push(0D);
            } else {
                methodWriter.visitInsn(Opcodes.ACONST_NULL);
            }
        } else {
            visit(irDeclarationNode.getExpressionNode(), writeScope);
        }

        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());
    }

    @Override
    public void visitReturn(ReturnNode irReturnNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irReturnNode.getLocation());

        if (irReturnNode.getExpressionNode() != null) {
            visit(irReturnNode.getExpressionNode(), writeScope);
        }

        methodWriter.returnValue();
    }

    @Override
    public void visitStatementExpression(StatementExpressionNode irStatementExpressionNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irStatementExpressionNode.getLocation());
        visit(irStatementExpressionNode.getExpressionNode(), writeScope);
        methodWriter.writePop(MethodWriter.getType(
                irStatementExpressionNode.getExpressionNode().getDecoration(IRDExpressionType.class).getType()).getSize());
    }

    @Override
    public void visitTry(TryNode irTryNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irTryNode.getLocation());

        Label tryBeginLabel = new Label();
        Label tryEndLabel = new Label();
        Label catchesEndLabel = new Label();

        methodWriter.mark(tryBeginLabel);

        visit(irTryNode.getBlockNode(), writeScope.newBlockScope());

        if (irTryNode.getBlockNode().doAllEscape() == false) {
            methodWriter.goTo(catchesEndLabel);
        }

        methodWriter.mark(tryEndLabel);

        List<CatchNode> catchNodes = irTryNode.getCatchNodes();

        for (int i = 0; i < catchNodes.size(); ++i) {
            CatchNode irCatchNode = catchNodes.get(i);
            Label catchJumpLabel = catchNodes.size() > 1 && i < catchNodes.size() - 1 ? catchesEndLabel : null;
            visit(irCatchNode, writeScope.newTryScope(tryBeginLabel, tryEndLabel, catchJumpLabel));
        }

        if (irTryNode.getBlockNode().doAllEscape() == false || catchNodes.size() > 1) {
            methodWriter.mark(catchesEndLabel);
        }
    }

    @Override
    public void visitCatch(CatchNode irCatchNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irCatchNode.getLocation());

        Variable variable = writeScope.defineVariable(irCatchNode.getExceptionType(), irCatchNode.getSymbol());

        Label jump = new Label();

        methodWriter.mark(jump);
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());

        BlockNode irBlockNode = irCatchNode.getBlockNode();

        if (irBlockNode != null) {
            visit(irBlockNode, writeScope.newTryScope(null, null, null));
        }

        methodWriter.visitTryCatchBlock(
                writeScope.getTryBeginLabel(), writeScope.getTryEndLabel(), jump, variable.getAsmType().getInternalName());

        if (writeScope.getCatchesEndLabel() != null && (irBlockNode == null || irBlockNode.doAllEscape() == false)) {
            methodWriter.goTo(writeScope.getCatchesEndLabel());
        }
    }

    @Override
    public void visitThrow(ThrowNode irThrowNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeStatementOffset(irThrowNode.getLocation());
        visit(irThrowNode.getExpressionNode(), writeScope);
        methodWriter.throwException();
    }

    @Override
    public void visitContinue(ContinueNode irContinueNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.goTo(writeScope.getContinueLabel());
    }

    @Override
    public void visitBreak(BreakNode irBreakNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.goTo(writeScope.getBreakLabel());
    }

    @Override
    public void visitBinaryImpl(BinaryImplNode irBinaryImplNode, WriteScope writeScope) {
        visit(irBinaryImplNode.getLeftNode(), writeScope);
        visit(irBinaryImplNode.getRightNode(), writeScope);
    }

    @Override
    public void visitUnaryMath(UnaryMathNode irUnaryMathNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irUnaryMathNode.getLocation());

        Operation operation = irUnaryMathNode.getOperation();

        if (operation == Operation.NOT) {
            Label fals = new Label();
            Label end = new Label();

            visit(irUnaryMathNode.getChildNode(), writeScope);

            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.push(false);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(true);
            methodWriter.mark(end);
        } else {
            visit(irUnaryMathNode.getChildNode(), writeScope);

            // Def calls adopt the wanted return value. If there was a narrowing cast,
            // we need to flag that so that it's done at runtime.
            int defFlags = 0;

            if (irUnaryMathNode.getOriginallyExplicit()) {
                defFlags |= DefBootstrap.OPERATOR_EXPLICIT_CAST;
            }

            Type actualType = MethodWriter.getType(irUnaryMathNode.getDecoration(IRDExpressionType.class).getType());
            Type childType = MethodWriter.getType(irUnaryMathNode.getChildNode().getDecoration(IRDExpressionType.class).getType());

            Class<?> unaryType = irUnaryMathNode.getUnaryType();

            if (operation == Operation.BWNOT) {
                if (unaryType == def.class) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("not", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                } else {
                    if (unaryType == int.class) {
                        methodWriter.push(-1);
                    } else if (unaryType == long.class) {
                        methodWriter.push(-1L);
                    } else {
                        throw new IllegalStateException("unexpected unary math operation [" + operation + "] " +
                                "for type [" + irUnaryMathNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName() + "]");
                    }

                    methodWriter.math(MethodWriter.XOR, actualType);
                }
            } else if (operation == Operation.SUB) {
                if (unaryType == def.class) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("neg", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                } else {
                    methodWriter.math(MethodWriter.NEG, actualType);
                }
            } else if (operation == Operation.ADD) {
                if (unaryType == def.class) {
                    org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(actualType, childType);
                    methodWriter.invokeDefCall("plus", descriptor, DefBootstrap.UNARY_OPERATOR, defFlags);
                }
            } else {
                throw new IllegalStateException("unexpected unary math operation [" + operation + "] " +
                        "for type [" + irUnaryMathNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName() + "]");
            }
        }
    }

    @Override
    public void visitBinaryMath(BinaryMathNode irBinaryMathNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irBinaryMathNode.getLocation());

        Operation operation = irBinaryMathNode.getOperation();
        ExpressionNode irLeftNode = irBinaryMathNode.getLeftNode();
        ExpressionNode irRightNode = irBinaryMathNode.getRightNode();

        if (operation == Operation.FIND || operation == Operation.MATCH) {
            visit(irRightNode, writeScope);
            visit(irLeftNode, writeScope);
            methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Pattern.class), WriterConstants.PATTERN_MATCHER);

            if (operation == Operation.FIND) {
                methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Matcher.class), WriterConstants.MATCHER_FIND);
            } else if (operation == Operation.MATCH) {
                methodWriter.invokeVirtual(org.objectweb.asm.Type.getType(Matcher.class), WriterConstants.MATCHER_MATCHES);
            } else {
                throw new IllegalStateException("unexpected binary math operation [" + operation + "] " +
                        "for type [" + irBinaryMathNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName() + "]");
            }
        } else {
            visit(irLeftNode, writeScope);
            visit(irRightNode, writeScope);

            if (irBinaryMathNode.getBinaryType() == def.class ||
                    (irBinaryMathNode.getShiftType() != null && irBinaryMathNode.getShiftType() == def.class)) {
                methodWriter.writeDynamicBinaryInstruction(irBinaryMathNode.getLocation(),
                        irBinaryMathNode.getDecoration(IRDExpressionType.class).getType(),
                        irLeftNode.getDecoration(IRDExpressionType.class).getType(),
                        irRightNode.getDecoration(IRDExpressionType.class).getType(),
                        operation, irBinaryMathNode.getFlags());
            } else {
                methodWriter.writeBinaryInstruction(
                        irBinaryMathNode.getLocation(), irBinaryMathNode.getDecoration(IRDExpressionType.class).getType(), operation);
            }
        }
    }

    @Override
    public void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irStringConcatenationNode.getLocation());
        methodWriter.writeNewStrings();

        for (ExpressionNode argumentNode : irStringConcatenationNode.getArgumentNodes()) {
            visit(argumentNode, writeScope);
            methodWriter.writeAppendStrings(argumentNode.getDecoration(IRDExpressionType.class).getType());
        }

        methodWriter.writeToStrings();
    }

    @Override
    public void visitBoolean(BooleanNode irBooleanNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irBooleanNode.getLocation());

        Operation operation = irBooleanNode.getOperation();
        ExpressionNode irLeftNode = irBooleanNode.getLeftNode();
        ExpressionNode irRightNode = irBooleanNode.getRightNode();

        if (operation == Operation.AND) {
            Label fals = new Label();
            Label end = new Label();

            visit(irLeftNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);
            visit(irRightNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.push(true);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(false);
            methodWriter.mark(end);
        } else if (operation == Operation.OR) {
            Label tru = new Label();
            Label fals = new Label();
            Label end = new Label();

            visit(irLeftNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFNE, tru);
            visit(irRightNode, writeScope);
            methodWriter.ifZCmp(Opcodes.IFEQ, fals);

            methodWriter.mark(tru);
            methodWriter.push(true);
            methodWriter.goTo(end);
            methodWriter.mark(fals);
            methodWriter.push(false);
            methodWriter.mark(end);
        } else {
            throw new IllegalStateException("unexpected boolean operation [" + operation + "] " +
                    "for type [" + irBooleanNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName() + "]");
        }
    }

    @Override
    public void visitComparison(ComparisonNode irComparisonNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irComparisonNode.getLocation());

        Operation operation = irComparisonNode.getOperation();
        ExpressionNode irLeftNode = irComparisonNode.getLeftNode();
        ExpressionNode irRightNode = irComparisonNode.getRightNode();

        visit(irLeftNode, writeScope);

        if (irRightNode instanceof NullNode == false) {
            visit(irRightNode, writeScope);
        }

        Label jump = new Label();
        Label end = new Label();

        boolean eq = (operation == Operation.EQ || operation == Operation.EQR);
        boolean ne = (operation == Operation.NE || operation == Operation.NER);
        boolean lt  = operation == Operation.LT;
        boolean lte = operation == Operation.LTE;
        boolean gt  = operation == Operation.GT;
        boolean gte = operation == Operation.GTE;

        boolean writejump = true;

        Class<?> comparisonType = irComparisonNode.getComparisonType();
        Type type = MethodWriter.getType(comparisonType);

        if (comparisonType == void.class || comparisonType == byte.class
                || comparisonType == short.class || comparisonType == char.class) {
            throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                    "for type [" + irComparisonNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName() + "]");
        } else if (comparisonType == boolean.class) {
            if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
            else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + irComparisonNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName() + "]");
            }
        } else if (comparisonType == int.class || comparisonType == long.class
                || comparisonType == float.class || comparisonType == double.class) {
            if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
            else if (lt) methodWriter.ifCmp(type, MethodWriter.LT, jump);
            else if (lte) methodWriter.ifCmp(type, MethodWriter.LE, jump);
            else if (gt) methodWriter.ifCmp(type, MethodWriter.GT, jump);
            else if (gte) methodWriter.ifCmp(type, MethodWriter.GE, jump);
            else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + irComparisonNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName() + "]");
            }

        } else if (comparisonType == def.class) {
            Type booleanType = Type.getType(boolean.class);
            Type descriptor = Type.getMethodType(booleanType,
                    MethodWriter.getType(irLeftNode.getDecoration(IRDExpressionType.class).getType()),
                    MethodWriter.getType(irRightNode.getDecoration(IRDExpressionType.class).getType()));

            if (eq) {
                if (irRightNode instanceof NullNode) {
                    methodWriter.ifNull(jump);
                } else if (irLeftNode instanceof NullNode == false && operation == Operation.EQ) {
                    methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    writejump = false;
                } else {
                    methodWriter.ifCmp(type, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (irRightNode instanceof NullNode) {
                    methodWriter.ifNonNull(jump);
                } else if (irLeftNode instanceof NullNode == false && operation == Operation.NE) {
                    methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    methodWriter.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    methodWriter.ifCmp(type, MethodWriter.NE, jump);
                }
            } else if (lt) {
                methodWriter.invokeDefCall("lt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (lte) {
                methodWriter.invokeDefCall("lte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (gt) {
                methodWriter.invokeDefCall("gt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (gte) {
                methodWriter.invokeDefCall("gte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + irComparisonNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName() + "]");
            }
        } else {
            if (eq) {
                if (irRightNode instanceof NullNode) {
                    methodWriter.ifNull(jump);
                } else if (operation == Operation.EQ) {
                    methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                    writejump = false;
                } else {
                    methodWriter.ifCmp(type, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (irRightNode instanceof NullNode) {
                    methodWriter.ifNonNull(jump);
                } else if (operation == Operation.NE) {
                    methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                    methodWriter.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    methodWriter.ifCmp(type, MethodWriter.NE, jump);
                }
            } else {
                throw new IllegalStateException("unexpected comparison operation [" + operation + "] " +
                        "for type [" + irComparisonNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName() + "]");
            }
        }

        if (writejump) {
            methodWriter.push(false);
            methodWriter.goTo(end);
            methodWriter.mark(jump);
            methodWriter.push(true);
            methodWriter.mark(end);
        }
    }

    @Override
    public void visitCast(CastNode irCastNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irCastNode.getChildNode(), writeScope);
        methodWriter.writeDebugInfo(irCastNode.getLocation());
        methodWriter.writeCast(irCastNode.getCast());
    }

    @Override
    public void visitInstanceof(InstanceofNode irInstanceofNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        ExpressionNode irChildNode = irInstanceofNode.getChildNode();

        visit(irChildNode, writeScope);

        Class<?> instanceType = irInstanceofNode.getInstanceType();
        Class<?> expressionType = irInstanceofNode.getDecoration(IRDExpressionType.class).getType();

        if (irInstanceofNode.getInstanceType() == def.class) {
            methodWriter.writePop(MethodWriter.getType(expressionType).getSize());
            methodWriter.push(true);
        } else if (irChildNode.getDecoration(IRDExpressionType.class).getType().isPrimitive()) {
            methodWriter.writePop(MethodWriter.getType(expressionType).getSize());
            methodWriter.push(PainlessLookupUtility.typeToBoxedType(instanceType).isAssignableFrom(
                    PainlessLookupUtility.typeToBoxedType(irChildNode.getDecoration(IRDExpressionType.class).getType())));
        } else {
            methodWriter.instanceOf(MethodWriter.getType(PainlessLookupUtility.typeToBoxedType(instanceType)));
        }
    }

    @Override
    public void visitConditional(ConditionalNode irConditionalNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irConditionalNode.getLocation());

        Label fals = new Label();
        Label end = new Label();

        visit(irConditionalNode.getConditionNode(), writeScope);
        methodWriter.ifZCmp(Opcodes.IFEQ, fals);

        visit(irConditionalNode.getLeftNode(), writeScope);
        methodWriter.goTo(end);
        methodWriter.mark(fals);
        visit(irConditionalNode.getRightNode(), writeScope);
        methodWriter.mark(end);
    }

    @Override
    public void visitElvis(ElvisNode irElvisNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irElvisNode.getLocation());

        Label end = new Label();

        visit(irElvisNode.getLeftNode(), writeScope);
        methodWriter.dup();
        methodWriter.ifNonNull(end);
        methodWriter.pop();
        visit(irElvisNode.getRightNode(), writeScope);
        methodWriter.mark(end);
    }

    @Override
    public void visitListInitialization(ListInitializationNode irListInitializationNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irListInitializationNode.getLocation());

        PainlessConstructor painlessConstructor = irListInitializationNode.getConstructor();
        methodWriter.newInstance(MethodWriter.getType(irListInitializationNode.getDecoration(IRDExpressionType.class).getType()));
        methodWriter.dup();
        methodWriter.invokeConstructor(
                Type.getType(painlessConstructor.javaConstructor.getDeclaringClass()),
                Method.getMethod(painlessConstructor.javaConstructor));

        for (ExpressionNode irArgumentNode : irListInitializationNode.getArgumentNodes()) {
            methodWriter.dup();
            visit(irArgumentNode, writeScope);
            methodWriter.invokeMethodCall(irListInitializationNode.getMethod());
            methodWriter.pop();
        }
    }

    @Override
    public void visitMapInitialization(MapInitializationNode irMapInitializationNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irMapInitializationNode.getLocation());

        PainlessConstructor painlessConstructor = irMapInitializationNode.getConstructor();
        methodWriter.newInstance(MethodWriter.getType(irMapInitializationNode.getDecoration(IRDExpressionType.class).getType()));
        methodWriter.dup();
        methodWriter.invokeConstructor(
                Type.getType(painlessConstructor.javaConstructor.getDeclaringClass()),
                Method.getMethod(painlessConstructor.javaConstructor));

        for (int index = 0; index < irMapInitializationNode.getArgumentsSize(); ++index) {
            methodWriter.dup();
            visit(irMapInitializationNode.getKeyNode(index), writeScope);
            visit(irMapInitializationNode.getValueNode(index), writeScope);
            methodWriter.invokeMethodCall(irMapInitializationNode.getMethod());
            methodWriter.pop();
        }
    }

    @Override
    public void visitNewArray(NewArrayNode irNewArrayNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irNewArrayNode.getLocation());

        List<ExpressionNode> irArgumentNodes = irNewArrayNode.getArgumentNodes();
        Class<?> expressionType = irNewArrayNode.getDecoration(IRDExpressionType.class).getType();

        if (irNewArrayNode.getInitialize()) {
            methodWriter.push(irNewArrayNode.getArgumentNodes().size());
            methodWriter.newArray(MethodWriter.getType(expressionType.getComponentType()));

            for (int index = 0; index < irArgumentNodes.size(); ++index) {
                ExpressionNode irArgumentNode = irArgumentNodes.get(index);

                methodWriter.dup();
                methodWriter.push(index);
                visit(irArgumentNode, writeScope);
                methodWriter.arrayStore(MethodWriter.getType(expressionType.getComponentType()));
            }
        } else {
            for (ExpressionNode irArgumentNode : irArgumentNodes) {
                visit(irArgumentNode, writeScope);
            }

            if (irArgumentNodes.size() > 1) {
                methodWriter.visitMultiANewArrayInsn(MethodWriter.getType(expressionType).getDescriptor(), irArgumentNodes.size());
            } else {
                methodWriter.newArray(MethodWriter.getType(expressionType.getComponentType()));
            }
        }
    }

    @Override
    public void visitNewObject(NewObjectNode irNewObjectNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irNewObjectNode.getLocation());

        methodWriter.newInstance(MethodWriter.getType(irNewObjectNode.getDecoration(IRDExpressionType.class).getType()));

        if (irNewObjectNode.getRead()) {
            methodWriter.dup();
        }

        for (ExpressionNode irArgumentNode : irNewObjectNode.getArgumentNodes()) {
            visit(irArgumentNode, writeScope);
        }

        PainlessConstructor painlessConstructor = irNewObjectNode.getConstructor();
        methodWriter.invokeConstructor(
                Type.getType(painlessConstructor.javaConstructor.getDeclaringClass()),
                Method.getMethod(painlessConstructor.javaConstructor));
    }

    @Override
    public void visitConstant(ConstantNode irConstantNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        Object constant = irConstantNode.getConstant();

        if      (constant instanceof String)    methodWriter.push((String)constant);
        else if (constant instanceof Double)    methodWriter.push((double)constant);
        else if (constant instanceof Float)     methodWriter.push((float)constant);
        else if (constant instanceof Long)      methodWriter.push((long)constant);
        else if (constant instanceof Integer)   methodWriter.push((int)constant);
        else if (constant instanceof Character) methodWriter.push((char)constant);
        else if (constant instanceof Short)     methodWriter.push((short)constant);
        else if (constant instanceof Byte)      methodWriter.push((byte)constant);
        else if (constant instanceof Boolean)   methodWriter.push((boolean)constant);
        else {
            throw new IllegalStateException("unexpected constant [" + constant + "]");
        }
    }

    @Override
    public void visitNull(NullNode irNullNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.visitInsn(Opcodes.ACONST_NULL);
    }

    @Override
    public void visitDefInterfaceReference(DefInterfaceReferenceNode irDefInterfaceReferenceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irDefInterfaceReferenceNode.getLocation());

        // place holder for functional interface receiver
        // which is resolved and replace at runtime
        methodWriter.push((String)null);

        for (String capture : irDefInterfaceReferenceNode.getCaptures()) {
            WriteScope.Variable variable = writeScope.getVariable(capture);
            methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ILOAD), variable.getSlot());
        }
    }

    @Override
    public void visitTypedInterfaceReference(TypedInterfaceReferenceNode irTypedInterfaceReferenceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irTypedInterfaceReferenceNode.getLocation());

        for (String capture : irTypedInterfaceReferenceNode.getCaptures()) {
            WriteScope.Variable variable = writeScope.getVariable(capture);
            methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ILOAD), variable.getSlot());
        }

        methodWriter.invokeLambdaCall(irTypedInterfaceReferenceNode.getReference());
    }

    @Override
    public void visitTypeCaptureReference(TypedCaptureReferenceNode irTypedCaptureReferenceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irTypedCaptureReferenceNode.getLocation());
        Variable captured = writeScope.getVariable(irTypedCaptureReferenceNode.getCaptures().get(0));

        methodWriter.visitVarInsn(captured.getAsmType().getOpcode(Opcodes.ILOAD), captured.getSlot());
        Type methodType = Type.getMethodType(MethodWriter.getType(
                irTypedCaptureReferenceNode.getDecoration(IRDExpressionType.class).getType()), captured.getAsmType());
        methodWriter.invokeDefCall(irTypedCaptureReferenceNode.getMethodName(), methodType, DefBootstrap.REFERENCE,
                irTypedCaptureReferenceNode.getDecoration(IRDExpressionType.class).getCanonicalTypeName());
    }

    @Override
    public void visitStatic(StaticNode irStaticNode, WriteScope writeScope) {
        // do nothing
    }

    @Override
    public void visitLoadVariable(LoadVariableNode irLoadVariableNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        Variable variable = writeScope.getVariable(irLoadVariableNode.getName());
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ILOAD), variable.getSlot());
    }

    @Override
    public void visitNullSafeSub(NullSafeSubNode irNullSafeSubNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irNullSafeSubNode.getLocation());

        Label end = new Label();
        methodWriter.dup();
        methodWriter.ifNull(end);
        visit(irNullSafeSubNode.getChildNode(), writeScope);
        methodWriter.mark(end);
    }

    @Override
    public void visitLoadDotArrayLengthNode(LoadDotArrayLengthNode irLoadDotArrayLengthNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadDotArrayLengthNode.getLocation());
        methodWriter.arrayLength();
    }

    @Override
    public void visitLoadDotDef(LoadDotDefNode irLoadDotDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadDotDefNode.getLocation());
        Type methodType = Type.getMethodType(
                MethodWriter.getType(irLoadDotDefNode.getDecoration(IRDExpressionType.class).getType()),
                MethodWriter.getType(def.class));
        methodWriter.invokeDefCall(irLoadDotDefNode.getValue(), methodType, DefBootstrap.LOAD);
    }

    @Override
    public void visitLoadDot(LoadDotNode irLoadDotNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadDotNode.getLocation());

        PainlessField painlessField = irLoadDotNode.getField();

        if (java.lang.reflect.Modifier.isStatic(painlessField.javaField.getModifiers())) {
            methodWriter.getStatic(Type.getType(painlessField.javaField.getDeclaringClass()),
                    painlessField.javaField.getName(), MethodWriter.getType(painlessField.typeParameter));
        } else {
            methodWriter.getField(Type.getType(painlessField.javaField.getDeclaringClass()),
                    painlessField.javaField.getName(), MethodWriter.getType(painlessField.typeParameter));
        }
    }

    @Override
    public void visitLoadDotShortcut(LoadDotShortcutNode irDotSubShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irDotSubShortcutNode.getLocation());

        PainlessMethod getterPainlessMethod = irDotSubShortcutNode.getGetter();
        methodWriter.invokeMethodCall(getterPainlessMethod);

        if (!getterPainlessMethod.returnType.equals(getterPainlessMethod.javaMethod.getReturnType())) {
            methodWriter.checkCast(MethodWriter.getType(getterPainlessMethod.returnType));
        }
    }

    @Override
    public void visitLoadListShortcut(LoadListShortcutNode irLoadListShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadListShortcutNode.getLocation());

        PainlessMethod getterPainlessMethod = irLoadListShortcutNode.getGetter();
        methodWriter.invokeMethodCall(getterPainlessMethod);

        if (getterPainlessMethod.returnType == getterPainlessMethod.javaMethod.getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getterPainlessMethod.returnType));
        }
    }

    @Override
    public void visitLoadMapShortcut(LoadMapShortcutNode irLoadMapShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadMapShortcutNode.getLocation());

        PainlessMethod getterPainlessMethod = irLoadMapShortcutNode.getGetter();
        methodWriter.invokeMethodCall(getterPainlessMethod);

        if (getterPainlessMethod.returnType != getterPainlessMethod.javaMethod.getReturnType()) {
            methodWriter.checkCast(MethodWriter.getType(getterPainlessMethod.returnType));
        }
    }

    @Override
    public void visitLoadFieldMember(LoadFieldMemberNode irLoadFieldMemberNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadFieldMemberNode.getLocation());

        if (irLoadFieldMemberNode.isStatic()) {
            methodWriter.getStatic(CLASS_TYPE, irLoadFieldMemberNode.getName(),
                    MethodWriter.getType(irLoadFieldMemberNode.getDecoration(IRDExpressionType.class).getType()));
        } else {
            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, irLoadFieldMemberNode.getName(),
                    MethodWriter.getType(irLoadFieldMemberNode.getDecoration(IRDExpressionType.class).getType()));
        }
    }

    @Override
    public void visitLoadBraceDef(LoadBraceDefNode irLoadBraceDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadBraceDefNode.getLocation());
        Type methodType = Type.getMethodType(
                MethodWriter.getType(irLoadBraceDefNode.getDecoration(IRDExpressionType.class).getType()),
                MethodWriter.getType(def.class),
                MethodWriter.getType(irLoadBraceDefNode.getIndexType()));
        methodWriter.invokeDefCall("arrayLoad", methodType, DefBootstrap.ARRAY_LOAD);
    }

    @Override
    public void visitLoadBrace(LoadBraceNode irLoadBraceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irLoadBraceNode.getLocation());
        methodWriter.arrayLoad(MethodWriter.getType(irLoadBraceNode.getDecoration(IRDExpressionType.class).getType()));
    }

    @Override
    public void visitStoreVariable(StoreVariableNode irStoreVariableNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreVariableNode.getChildNode(), writeScope);

        Variable variable = writeScope.getVariable(irStoreVariableNode.getName());
        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());
    }

    @Override
    public void visitStoreDotDef(StoreDotDefNode irStoreDotDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreDotDefNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreDotDefNode.getLocation());
        Type methodType = Type.getMethodType(
                MethodWriter.getType(void.class),
                MethodWriter.getType(def.class),
                MethodWriter.getType(irStoreDotDefNode.getStoreType()));
        methodWriter.invokeDefCall(irStoreDotDefNode.getValue(), methodType, DefBootstrap.STORE);
    }

    @Override
    public void visitStoreDot(StoreDotNode irStoreDotNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreDotNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreDotNode.getLocation());

        PainlessField painlessField = irStoreDotNode.getField();

        if (java.lang.reflect.Modifier.isStatic(painlessField.javaField.getModifiers())) {
            methodWriter.putStatic(Type.getType(painlessField.javaField.getDeclaringClass()),
                    painlessField.javaField.getName(), MethodWriter.getType(painlessField.typeParameter));
        } else {
            methodWriter.putField(Type.getType(painlessField.javaField.getDeclaringClass()),
                    painlessField.javaField.getName(), MethodWriter.getType(painlessField.typeParameter));
        }
    }

    @Override
    public void visitStoreDotShortcut(StoreDotShortcutNode irDotSubShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irDotSubShortcutNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irDotSubShortcutNode.getLocation());
        methodWriter.invokeMethodCall(irDotSubShortcutNode.getSetter());
        methodWriter.writePop(MethodWriter.getType(irDotSubShortcutNode.getSetter().returnType).getSize());
    }

    @Override
    public void visitStoreListShortcut(StoreListShortcutNode irStoreListShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreListShortcutNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreListShortcutNode.getLocation());
        methodWriter.invokeMethodCall(irStoreListShortcutNode.getSetter());
        methodWriter.writePop(MethodWriter.getType(irStoreListShortcutNode.getSetter().returnType).getSize());
    }

    @Override
    public void visitStoreMapShortcut(StoreMapShortcutNode irStoreMapShortcutNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreMapShortcutNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreMapShortcutNode.getLocation());
        methodWriter.invokeMethodCall(irStoreMapShortcutNode.getSetter());
        methodWriter.writePop(MethodWriter.getType(irStoreMapShortcutNode.getSetter().returnType).getSize());
    }

    @Override
    public void visitStoreFieldMember(StoreFieldMemberNode irStoreFieldMemberNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        if (irStoreFieldMemberNode.isStatic() == false) {
            methodWriter.loadThis();
        }

        visit(irStoreFieldMemberNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreFieldMemberNode.getLocation());

        if (irStoreFieldMemberNode.isStatic()) {
            methodWriter.putStatic(CLASS_TYPE,
                    irStoreFieldMemberNode.getName(), MethodWriter.getType(irStoreFieldMemberNode.getStoreType()));
        } else {
            methodWriter.putField(CLASS_TYPE,
                    irStoreFieldMemberNode.getName(), MethodWriter.getType(irStoreFieldMemberNode.getStoreType()));
        }
    }

    @Override
    public void visitStoreBraceDef(StoreBraceDefNode irStoreBraceDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreBraceDefNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreBraceDefNode.getLocation());
        Type methodType = Type.getMethodType(
                MethodWriter.getType(void.class),
                MethodWriter.getType(def.class),
                MethodWriter.getType(irStoreBraceDefNode.getIndexType()),
                MethodWriter.getType(irStoreBraceDefNode.getStoreType()));
        methodWriter.invokeDefCall("arrayStore", methodType, DefBootstrap.ARRAY_STORE);
    }

    @Override
    public void visitStoreBrace(StoreBraceNode irStoreBraceNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irStoreBraceNode.getChildNode(), writeScope);

        methodWriter.writeDebugInfo(irStoreBraceNode.getLocation());
        methodWriter.arrayStore(MethodWriter.getType(irStoreBraceNode.getStoreType()));
    }

    @Override
    public void visitInvokeCallDef(InvokeCallDefNode irInvokeCallDefNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irInvokeCallDefNode.getLocation());

        // its possible to have unknown functional interfaces
        // as arguments that require captures; the set of
        // captures with call arguments is ambiguous so
        // additional information is encoded to indicate
        // which are values are arguments and which are captures
        StringBuilder defCallRecipe = new StringBuilder();
        List<Object> boostrapArguments = new ArrayList<>();
        List<Class<?>> typeParameters = new ArrayList<>();
        int capturedCount = 0;

        // add an Object class as a placeholder type for the receiver
        typeParameters.add(Object.class);

        for (int i = 0; i < irInvokeCallDefNode.getArgumentNodes().size(); ++i) {
            ExpressionNode irArgumentNode = irInvokeCallDefNode.getArgumentNodes().get(i);
            visit(irArgumentNode, writeScope);

            typeParameters.add(irArgumentNode.getDecoration(IRDExpressionType.class).getType());

            // handle the case for unknown functional interface
            // to hint at which values are the call's arguments
            // versus which values are captures
            if (irArgumentNode instanceof DefInterfaceReferenceNode) {
                DefInterfaceReferenceNode defInterfaceReferenceNode = (DefInterfaceReferenceNode)irArgumentNode;
                boostrapArguments.add(defInterfaceReferenceNode.getDefReferenceEncoding());

                // the encoding uses a char to indicate the number of captures
                // where the value is the number of current arguments plus the
                // total number of captures for easier capture count tracking
                // when resolved at runtime
                char encoding = (char)(i + capturedCount);
                defCallRecipe.append(encoding);
                capturedCount += defInterfaceReferenceNode.getCaptures().size();

                for (String capturedName : defInterfaceReferenceNode.getCaptures()) {
                    Variable capturedVariable = writeScope.getVariable(capturedName);
                    typeParameters.add(capturedVariable.getType());
                }
            }
        }

        Type[] asmParameterTypes = new Type[typeParameters.size()];

        for (int index = 0; index < asmParameterTypes.length; ++index) {
            asmParameterTypes[index] = MethodWriter.getType(typeParameters.get(index));
        }

        Type methodType = Type.getMethodType(MethodWriter.getType(
                irInvokeCallDefNode.getDecoration(IRDExpressionType.class).getType()), asmParameterTypes);

        boostrapArguments.add(0, defCallRecipe.toString());
        methodWriter.invokeDefCall(irInvokeCallDefNode.getName(), methodType, DefBootstrap.METHOD_CALL, boostrapArguments.toArray());
    }

    @Override
    public void visitInvokeCall(InvokeCallNode irInvokeCallNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irInvokeCallNode.getLocation());

        if (irInvokeCallNode.getBox().isPrimitive()) {
            methodWriter.box(MethodWriter.getType(irInvokeCallNode.getBox()));
        }

        for (ExpressionNode irArgumentNode : irInvokeCallNode.getArgumentNodes()) {
            visit(irArgumentNode, writeScope);
        }

        methodWriter.invokeMethodCall(irInvokeCallNode.getMethod());
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        methodWriter.writeDebugInfo(irInvokeCallMemberNode.getLocation());

        LocalFunction localFunction = irInvokeCallMemberNode.getLocalFunction();
        PainlessMethod importedMethod = irInvokeCallMemberNode.getImportedMethod();
        PainlessClassBinding classBinding = irInvokeCallMemberNode.getClassBinding();
        PainlessInstanceBinding instanceBinding = irInvokeCallMemberNode.getInstanceBinding();
        List<ExpressionNode> irArgumentNodes = irInvokeCallMemberNode.getArgumentNodes();

        if (localFunction != null) {
            if (localFunction.isStatic() == false) {
                methodWriter.loadThis();
            }

            for (ExpressionNode irArgumentNode : irArgumentNodes) {
                visit(irArgumentNode, writeScope);
            }

            if (localFunction.isStatic()) {
                methodWriter.invokeStatic(CLASS_TYPE, localFunction.getAsmMethod());
            } else {
                methodWriter.invokeVirtual(CLASS_TYPE, localFunction.getAsmMethod());
            }
        } else if (importedMethod != null) {
            for (ExpressionNode irArgumentNode : irArgumentNodes) {
                visit(irArgumentNode, writeScope);
            }

            methodWriter.invokeStatic(Type.getType(importedMethod.targetClass),
                    new Method(importedMethod.javaMethod.getName(), importedMethod.methodType.toMethodDescriptorString()));
        } else if (classBinding != null) {
            Type type = Type.getType(classBinding.javaConstructor.getDeclaringClass());
            int classBindingOffset = irInvokeCallMemberNode.getClassBindingOffset();
            int javaConstructorParameterCount = classBinding.javaConstructor.getParameterCount() - classBindingOffset;
            String bindingName = irInvokeCallMemberNode.getBindingName();

            Label nonNull = new Label();

            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);
            methodWriter.ifNonNull(nonNull);
            methodWriter.loadThis();
            methodWriter.newInstance(type);
            methodWriter.dup();

            if (classBindingOffset == 1) {
                methodWriter.loadThis();
            }

            for (int argument = 0; argument < javaConstructorParameterCount; ++argument) {
                visit(irArgumentNodes.get(argument), writeScope);
            }

            methodWriter.invokeConstructor(type, Method.getMethod(classBinding.javaConstructor));
            methodWriter.putField(CLASS_TYPE, bindingName, type);

            methodWriter.mark(nonNull);
            methodWriter.loadThis();
            methodWriter.getField(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < classBinding.javaMethod.getParameterCount(); ++argument) {
                visit(irArgumentNodes.get(argument + javaConstructorParameterCount), writeScope);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(classBinding.javaMethod));
        } else if (instanceBinding != null) {
            Type type = Type.getType(instanceBinding.targetInstance.getClass());
            String bindingName = irInvokeCallMemberNode.getBindingName();

            methodWriter.loadThis();
            methodWriter.getStatic(CLASS_TYPE, bindingName, type);

            for (int argument = 0; argument < instanceBinding.javaMethod.getParameterCount(); ++argument) {
                visit(irArgumentNodes.get(argument), writeScope);
            }

            methodWriter.invokeVirtual(type, Method.getMethod(instanceBinding.javaMethod));
        } else {
            throw new IllegalStateException("invalid unbound call");
        }
    }

    @Override
    public void visitFlipArrayIndex(FlipArrayIndexNode irFlipArrayIndexNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irFlipArrayIndexNode.getChildNode(), writeScope);

        Label noFlip = new Label();
        methodWriter.dup();
        methodWriter.ifZCmp(Opcodes.IFGE, noFlip);
        methodWriter.swap();
        methodWriter.dupX1();
        methodWriter.arrayLength();
        methodWriter.visitInsn(Opcodes.IADD);
        methodWriter.mark(noFlip);
    }

    @Override
    public void visitFlipCollectionIndex(FlipCollectionIndexNode irFlipCollectionIndexNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        visit(irFlipCollectionIndexNode.getChildNode(), writeScope);

        Label noFlip = new Label();
        methodWriter.dup();
        methodWriter.ifZCmp(Opcodes.IFGE, noFlip);
        methodWriter.swap();
        methodWriter.dupX1();
        methodWriter.invokeInterface(WriterConstants.COLLECTION_TYPE, WriterConstants.COLLECTION_SIZE);
        methodWriter.visitInsn(Opcodes.IADD);
        methodWriter.mark(noFlip);
    }

    @Override
    public void visitFlipDefIndex(FlipDefIndexNode irFlipDefIndexNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();

        methodWriter.dup();
        visit(irFlipDefIndexNode.getChildNode(), writeScope);
        Type methodType = Type.getMethodType(
                MethodWriter.getType(irFlipDefIndexNode.getChildNode().getDecoration(IRDExpressionType.class).getType()),
                MethodWriter.getType(def.class),
                MethodWriter.getType(irFlipDefIndexNode.getChildNode().getDecoration(IRDExpressionType.class).getType()));
        methodWriter.invokeDefCall("normalizeIndex", methodType, DefBootstrap.INDEX_NORMALIZE);
    }

    @Override
    public void visitDup(DupNode irDupNode, WriteScope writeScope) {
        MethodWriter methodWriter = writeScope.getMethodWriter();
        visit(irDupNode.getChildNode(), writeScope);
        methodWriter.writeDup(irDupNode.getSize(), irDupNode.getDepth());
    }
}
