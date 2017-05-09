namespace PiOptics.Ast
{
    using System;

    public class BinaryExpression : AstNode
    {
        public override bool HasChildren()
        {
            return Operator == Operator.And || Operator == Operator.Or;
        }
        public BinaryExpression(Operator op, AstNode left, AstNode right)
        {
            this.Operator = op;
            this.Left = left;
            this.Right = right;
        }

        public Operator Operator { get; set; }

        public AstNode Left { get; set; }

        public AstNode Right { get; set; }

        public override string ToString()
        {
            return string.Format(
                "{0} {1} {2}",
                this.Left,
                this.Operator.GetDescription(),
                this.Right);
        }
    }
}
