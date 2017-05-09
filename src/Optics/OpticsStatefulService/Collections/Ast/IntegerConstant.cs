namespace PiOptics.Ast
{
    public class IntegerConstant : AstNode
    {
        public IntegerConstant(int value)
        {
            this.Value = value;
        }

        public int Value { get; set; }

        public override string ToString()
        {
            return this.Value.ToString();
        }
    }
}
