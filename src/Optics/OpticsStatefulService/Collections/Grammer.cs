namespace PiOptics
{
    using Ast;
    using Sprache;
    using System;
    using System.ComponentModel;
    using System.Reflection;
    public enum Operator
    {
        [Description("=")]
        Equal,

        [Description("!")]
        NotEqual,

        [Description("and")]
        And,

        [Description("or")]
        Or
    }

    internal static class EnumExtensions
    {
        public static string GetDescription(this Enum source)
        {
            return source
                .GetType()
                .GetField(source.ToString())
                .GetCustomAttribute<DescriptionAttribute>()
                .Description;
        }
    }


    /// <summary>
    /// This builds a parsing grammer from out to in that supports the following language
    /// 
    /// conditional expressions
    /// PROPERTY = "value" - find properties that match value
    /// PROPERTY ! "value  - find properties that do not match value
    /// 
    /// //conditional grouping
    /// conditional AND conditional - find properties where both conditions match
    /// conditional OR conditional - find properties where either condition matches
    /// 
    /// //sub conditional grouping
    /// (conditional grouping) AND|OR (conditional grouping) apply and/or logic after results for the grouping within params is resolved.
    /// //no limit to number of params groupings.
    /// </summary>
    public static class Grammer
    {
        private static readonly Parser<char> Comma =
            Parse
                .Char(',')
                .Token();

        private static readonly Parser<char> OpenParenthesis =
            Parse
                .Char('(')
                .Token();

        private static readonly Parser<char> CloseParenthesis =
            Parse
                .Char(')')
                .Token();

        private static readonly Parser<AstNode> Integer =
            Parse
                .Number
                .Select(n => new IntegerConstant(int.Parse(n)))
                .Token();

        private static readonly Parser<AstNode> Null =
            Parse
                .IgnoreCase("null")
                .Return(new NullConstant())
                .Token();

        private static readonly Parser<AstNode> Variable =
            Parse
            .AnyChar
            .Except(Parse.WhiteSpace.Or(OpenParenthesis.Or(CloseParenthesis.Or(Parse.Char('"')))))
            .AtLeastOnce()
            .Text()
            .Where(t=>!Char.IsNumber(t[0]))
            .Select(t => new Variable(t))
            .Token();

        private static Parser<AstNode> Value
        {
            get
            {
                var e = from l in Parse.Char('"')
                        from v in Parse.AnyChar.Except(Parse.Char('"')).Many().Text()
                        from r in Parse.Char('"')
                        select new Value(v);
                return e;
            }
        }

        private static readonly Parser<Operator> Eq =
            MakeOperator(Operator.Equal);


        private static readonly Parser<Operator> NEq =
            MakeOperator(Operator.NotEqual);

        private static readonly Parser<Operator> And =
            MakeOperator(Operator.And);

        private static readonly Parser<Operator> Or =
            MakeOperator(Operator.Or);

        private static readonly Parser<AstNode> InExpressions =
            Parse
                .Ref(() => Expression)
                .DelimitedBy(Comma)
                .Contained(OpenParenthesis, CloseParenthesis)
                .Select(values => new ExpressionsList(values));

        private static readonly Parser<AstNode> ExpressionInParenthesis =
            Parse
                .Ref(() => Expression)
                .Contained(OpenParenthesis, CloseParenthesis)
                .Select(value => new GroupExpression(value))
            ;

        private static readonly Parser<AstNode> Primary =
            Variable
                .Or(Value)
                .Or(Integer)
                .Or(Null)
                .Or(ExpressionInParenthesis)
                .Token();

        private static readonly Parser<AstNode> Equality =
            Parse
                .ChainOperator(Eq, Primary, MakeBinary);

        private static readonly Parser<AstNode> NotEquality =
            Parse
                .ChainOperator(NEq, Equality, MakeBinary);

        private static readonly Parser<AstNode> ConditionalAnd =
            Parse
                .ChainOperator(And, NotEquality, MakeBinary);

        private static readonly Parser<AstNode> ConditionalOr =
            Parse
                .ChainOperator(Or, ConditionalAnd, MakeBinary);

        private static readonly Parser<AstNode> Expression =
            ConditionalOr;

        public static AstNode ParseExpression(string str)
        {
            return Expression.End().Parse(str);
        }

        private static Parser<Operator> MakeOperator(Operator op)
        {
            return MakeOperator(op.GetDescription(), op);
        }

        private static Parser<Operator> MakeOperator(string str, Operator op)
        {
            return Parse.String(str).Return(op).Token();
        }

        private static AstNode MakeBinary(Operator op, AstNode left, AstNode right)
        {
            return new BinaryExpression(op, left, right);
        }
    }
}