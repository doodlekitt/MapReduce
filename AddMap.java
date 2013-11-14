import java.io.*;
import java.util.*;

public class AddMap implements MapClass{

    public Map.Entry<Integer, Integer> map(String input){
        Integer value = Integer.valueOf(input);
        Map.Entry<Integer, Integer> entry =
            new AbstractMap.SimpleEntry<Integer, Integer>(0, value);
	return entry;
    }

    public Map.Entry<?, ?> reduce(Map.Entry<?, List<?>> input){
	Integer sum = 0;
	for(Object i : input.getValue()) {
            sum += ((Integer) i).intValue();
        }
	return new AbstractMap.SimpleEntry<Integer, Integer>(0, sum);
    }
}
