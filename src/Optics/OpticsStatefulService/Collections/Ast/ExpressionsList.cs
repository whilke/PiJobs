namespace PiOptics.Ast
{
    using System.Collections.Generic;
    using System.Linq;

    public class ExpressionsList : AstNode
    {
        public ExpressionsList(IEnumerable<AstNode> values)
        {
            this.Values = values.ToList();
        }

        public List<AstNode> Values { get; set; }

        public override string ToString()
        {
            return "(" + string.Join(", ", this.Values) + ")";
        }
    }
}
