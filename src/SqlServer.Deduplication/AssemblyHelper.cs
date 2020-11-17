using System.Reflection;

static class AssemblyHelper
{
    static AssemblyHelper()
    {
        Current = typeof(AssemblyHelper).Assembly;
        Name = Current.GetName().Name!;
    }

    public static readonly Assembly Current;

    public static readonly string Name;
}