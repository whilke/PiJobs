namespace PiOptics.Ast
{
    /// <summary>
    /// Abstarct syntax tree node base class
    /// </summary>
    public abstract class AstNode
    {
        public virtual bool HasChildren()
        {
            return false;
        }
    }
}
