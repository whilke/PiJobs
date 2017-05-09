using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PiOptics.Ast
{
    public class AstWalker
    {
//        Action<BinaryExpression, BinaryExpression, Operator> _callback;

        public Action<Operator> Opcallback
        {
            get; set;
        }

        public Action<string, string> AssignCallback
        {
            get;set;
        }

        public Action<string, string> NotAssignCallback
        {
            get; set;
        }


        public Action StartGroupCallback
        {
            get;set;
        }

        public void Walk(AstNode node)
        {
            if (node is BinaryExpression)
                Walk(node as BinaryExpression);

            if (node is GroupExpression)
                Walk(node as GroupExpression);
            return; 
        }

        public void Walk(GroupExpression node)
        {
            StartGroupCallback();
            Walk(node.Value);
        }

        public void Walk(BinaryExpression node)
        {
            switch(node.Operator)
            {
                case Operator.Equal:
                    {
                        AssignCallback(node.Left.ToString(), node.Right.ToString());
                        break;
                    }
                case Operator.NotEqual:
                    {
                        NotAssignCallback(node.Left.ToString(), node.Right.ToString());
                        break;
                    }
                case Operator.Or:
                case Operator.And:
                    {
                        Walk(node.Left);
                        Opcallback(node.Operator);
                        Walk(node.Right);
                        break;
                    }
                default:
                    {
                        break;
                    }
            }
        }
    }
}
