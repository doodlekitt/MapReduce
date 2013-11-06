import java.io.*;
import java.lang.*;
import java.util.*;

public static class Tuple{
    public String key;
    public int count;

    public Tuple(String key, int count){
	this.key = key;
	this.count = count;
    }
}

public class WordCount extends MapClass{

    public Object map(String input){
	String tuple = input + " 1";
	return ans;	
    }
    public Object combine(String[] input){
	Hashtable<String, int> comb = new Hashtable<String, int>();
        BufferedReader in = null;
        String kv = null;
        for(int i = 0; i < input.length; i++){
            in = new BufferedReader(new FileInputStream(input[i]);
            while(true){
                kv = in.readLine();
                if(kv == null) break;
                else{
		    String[] tuple = kv.split();
		    String key = tuple[0];
		    int count = Integer.valueOf(tuple[1]).intValue();
		    if(comb.get() != null){
			int temp = comb.get(key) + count;
			comb.put(key, temp);			
		    }else{
			comb.put(key, count);
		    }
                }
            }
            in.close();
        }
        return comb;
    }
    public Object reduce(Object input){
	String wordcount = null;
	Hashtable<String, int> in = (Hashtable<String, int>) input;
	for(String word : in.keySet()){
	    wordcount += word + ": " + in.get(word)+"\n";
	} 
	return wordcount;
    }

}
