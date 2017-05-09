namespace PiOptics.Ast
{
    using System.Collections.Generic;
    using System.Linq;

    public class GroupExpression : AstNode
    {
        public GroupExpression(AstNode value)
        {
            this.Value = value;
        }

        public AstNode Value { get; set; }

        public override string ToString()
        {
            return "(" + this.Value + ")";
        }
    }
}
