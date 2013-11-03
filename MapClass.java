//Not sure if it needs to extend but there ya go

public interface MapClass extends java.io.Serializable 
{
    public Object map(String input);
    public Object combine(Object input);
    public Object reduce(Object input);
}
