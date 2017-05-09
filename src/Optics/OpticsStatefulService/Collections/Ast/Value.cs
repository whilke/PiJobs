using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiOptics.Ast
{
    public class Value : AstNode
    {
        public string V { get; set; }

        public Value(string v)
        {
            V = v;
        }

        public override string ToString()
        {
            return V;
        }
    }
}
