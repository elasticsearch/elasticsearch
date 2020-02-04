/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.parser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DiagnosticErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.dfa.DFA;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;

public class EqlParser {

    private static final Logger log = LogManager.getLogger();

    private final boolean DEBUG = false;

    /**
     * Parses an EQL statement into execution plan
     * @param eql - the EQL statement
     */
    public LogicalPlan createStatement(String eql) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as statement: {}", eql);
        }
        return invokeParser(eql, EqlBaseParser::singleStatement, AstBuilder::plan);
    }

    public Expression createExpression(String expression) {
        if (log.isDebugEnabled()) {
            log.debug("Parsing as expression: {}", expression);
        }

        return invokeParser(expression, EqlBaseParser::singleExpression, AstBuilder::expression);
    }

    private <T> T invokeParser(String eql,
            Function<EqlBaseParser, ParserRuleContext> parseFunction,
                               BiFunction<AstBuilder, ParserRuleContext, T> visitor) {
        try {
            EqlBaseLexer lexer = new EqlBaseLexer(new ANTLRInputStream(eql));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            EqlBaseParser parser = new EqlBaseParser(tokenStream);

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames())));

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);

            if (DEBUG) {
                debug(parser);
                tokenStream.fill();

                for (Token t : tokenStream.getTokens()) {
                    String symbolicName = EqlBaseLexer.VOCABULARY.getSymbolicName(t.getType());
                    String literalName = EqlBaseLexer.VOCABULARY.getLiteralName(t.getType());
                    log.info(format(Locale.ROOT, "  %-15s '%s'",
                        symbolicName == null ? literalName : symbolicName,
                        t.getText()));
                }
            }

            ParserRuleContext tree = parseFunction.apply(parser);

            if (DEBUG) {
                log.info("Parse tree {} " + tree.toStringTree());
            }

            return visitor.apply(new AstBuilder(), tree);
        } catch (StackOverflowError e) {
            throw new ParsingException("EQL statement is too large, " +
                "causing stack overflow when generating the parsing tree: [{}]", eql);
        }
    }

    private static void debug(EqlBaseParser parser) {

        // when debugging, use the exact prediction mode (needed for diagnostics as well)
        parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);

        parser.addParseListener(parser.new TraceListener());

        parser.addErrorListener(new DiagnosticErrorListener(false) {
            @Override
            public void reportAttemptingFullContext(Parser recognizer, DFA dfa,
                    int startIndex, int stopIndex, BitSet conflictingAlts, ATNConfigSet configs) {}

            @Override
            public void reportContextSensitivity(Parser recognizer, DFA dfa,
                    int startIndex, int stopIndex, int prediction, ATNConfigSet configs) {}
        });
    }

    private class PostProcessor extends EqlBaseBaseListener {
        private final List<String> ruleNames;

        PostProcessor(List<String> ruleNames) {
            this.ruleNames = ruleNames;
        }


        @Override
        public void exitFunctionExpression(EqlBaseParser.FunctionExpressionContext context) {
            Token token = context.name;
            String functionName = token.getText();

            switch (functionName) {
                case "add":
                case "between":
                case "cidrMatch":
                case "concat":
                case "divide":
                case "endsWith":
                case "indexOf":
                case "length":
                case "match":
                case "modulo":
                case "multiply":
                case "number":
                case "startsWith":
                case "string":
                case "stringContains":
                case "substring":
                case "subtract":
                case "wildcard":
                    break;

                case "arrayContains":
                case "arrayCount":
                case "arraySearch":
                    throw new ParsingException(
                        "unsupported function " + functionName,
                        null,
                        token.getLine(),
                        token.getCharPositionInLine());

                default:
                    throw new ParsingException(
                        "unknown function " + functionName,
                        null,
                        token.getLine(),
                        token.getCharPositionInLine());
            }
        }

        @Override
        public void exitJoin(EqlBaseParser.JoinContext context) {
            Token token = context.JOIN().getSymbol();
            throw new ParsingException(
                "join is not supported",
                null,
                token.getLine(),
                token.getCharPositionInLine());
        }

        @Override
        public void exitPipe(EqlBaseParser.PipeContext context) {
            Token token = context.PIPE().getSymbol();
            throw new ParsingException(
                "pipes are not supported",
                null,
                token.getLine(),
                token.getCharPositionInLine());
        }

        @Override
        public void exitProcessCheck(EqlBaseParser.ProcessCheckContext context) {
            Token token = context.relationship;
            throw new ParsingException(
                "process relationships are not supported",
                null,
                token.getLine(),
                token.getCharPositionInLine());
        }

        @Override
        public void exitSequence(EqlBaseParser.SequenceContext context) {
            Token token = context.SEQUENCE().getSymbol();
            throw new ParsingException(
                "sequence is not supported",
                null,
                token.getLine(),
                token.getCharPositionInLine());
        }

        @Override
        public void exitQualifiedName(EqlBaseParser.QualifiedNameContext context) {
            if (context.INTEGER_VALUE().size() > 0) {
                Token firstIndex = context.INTEGER_VALUE(0).getSymbol();
                throw new ParsingException(
                    "array indexes are not supported",
                    null,
                    firstIndex.getLine(),
                    firstIndex.getCharPositionInLine());
            }
        }
    }

    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener() {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line,
                int charPositionInLine, String message, RecognitionException e) {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };
}
