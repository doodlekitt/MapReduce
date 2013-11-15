import java.io.*;
import java.lang.*;
import java.util.*;

public class MRReader{

    public static void main (String[] args) throws IOException{
	// Parse args
	if(args.length != 1){
	    System.out.println("Expecting File Name");
	    return;
	}
	try{
	    ObjectInputStream oin = 
		new ObjectInputStream(new FileInputStream(args[0]));
	    Hashtable<Object, Object> ans = 
		(Hashtable<Object, Object>) oin.readObject();
	    String answer = ans.toString();
	    System.out.println(answer);
	    oin.close();
	} catch (Exception e){
	   System.out.println(e);
        }
	
    }	

}
