import java.util.*;

public interface MapClass
{
    public Map.Entry<Object, Object> map(String input);
    public Map.Entry<Object, Object> reduce(Map.Entry<Object, List<Object>> input);
}
