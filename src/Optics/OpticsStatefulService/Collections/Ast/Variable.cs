using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiOptics.Ast
{
    public class Variable : AstNode
    {
        public string Value { get; set; }

        public Variable(string v)
        {
            Value = v;
        }

        public override string ToString()
        {
            return Value;
        }
    }
}
