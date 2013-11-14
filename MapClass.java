import java.util.*;

public interface MapClass extends java.io.Serializable
{
    public Map.Entry<?, ?> map(String input);
    public Map.Entry<?, ?> reduce(Map.Entry<?, List<?>> input);
}
