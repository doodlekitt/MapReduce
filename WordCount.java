import java.io.*;
import java.lang.*;
import java.util.*;

public class WordCount implements MapClass{

    public Map.Entry<Object, Object> map(String input){
	Map.Entry<Object, Object> entry = 
	    new AbstractMap.SimpleEntry<Object, Object>(input, 1);
    	return entry;
    }

    public Map.Entry<Object, Object> reduce(Map.Entry<Object, List<Object>> input){
	Integer sum = 0;
	for(Object i: input.getValue()) {
	    sum +=((Integer) i).intValue();
	}
	String k = (String)input.getKey();
	return new AbstractMap.SimpleEntry<Object, Object>(k, sum);
    }

}
