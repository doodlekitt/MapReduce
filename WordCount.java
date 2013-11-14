import java.io.*;
import java.lang.*;
import java.util.*;

public class WordCount implements MapClass{

    public Map.Entry<String, Integer> map(String input){
	Map.Entry<String, Integer> entry = 
	    new AbstractMap.SimpleEntry<String, Integer>(input, 1);
    	return entry;
    }

    public Map.Entry<?, ?> reduce(Map.Entry<?, List<?>> input){
	Integer sum = 0;
	for(Object i: input.getValue()) {
	    sum +=((Integer) i).intValue();
	}
	String k = (String)input.getKey();
	return new AbstractMap.SimpleEntry<String, Integer>(k, sum);
    }

}
