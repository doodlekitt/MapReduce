import java.util.*;

public interface MapClass extends java.io.Serializable
{
    public Map.Entry<Object, Object> map(String input);
    public Map.Entry<Object, Object> reduce(Map.Entry<Object, List<Object>> input);
}
