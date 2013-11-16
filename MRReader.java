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
	FileInputStream fin = new FileInputStream(args[0]);
	try{
	    ObjectInputStream oin = 
		new ObjectInputStream(fin);
            @SuppressWarnings("unchecked")
	    Hashtable<Object, Object> ans = 
		(Hashtable<Object, Object>) oin.readObject();
	    String answer = ans.toString();
	    System.out.println(answer);
	    fin.close();
	} catch (Exception e){
	   System.out.println(e);
        }
    }	
}
