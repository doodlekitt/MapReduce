import java.io.*;
import java.util.*;
import java.lang.*;

public class AddMap extends MapClass{

    public Object map(String input){
	return input;	
    }
    public Object combine(String[] input){
	Integer sum = 0;
	BufferedReader in = null;
	String num = null;
	for(int i = 0; i < input.length; i++){
	    in = new BufferedReader(new FileInputStream(input[i]);
	    while(true){
		num = in.readLine();
		if(num == null) break;
		else{
		    sum += Integer.valueOf(num);
		}
	    }
	    in.close();
	}
	return sum;	
    }
    public Object reduce(Object input){
	
    }

}
