import java.io.*;
import java.util.*;

public class AddMap implements MapClass{

    public Map.Entry<Object, Object> map(String input){
        Integer value = 0;
        try {
            value = Integer.parseInt(input);
        } catch (Exception e) {
            value = 0;
        }
        Map.Entry<Object, Object> entry =
            new AbstractMap.SimpleEntry<Object, Object>(0, value);
	return entry;
    }

    public Map.Entry<Object, Object> reduce(Map.Entry<Object, List<Object>> input){
        Integer sum = 0;
        try {
	    for(Object i : input.getValue()) {
                sum += ((Integer) i).intValue();
            }
        } catch (Exception e) {
            sum = 0;
        }
	return new AbstractMap.SimpleEntry<Object, Object>(0, sum);
    }
}
