import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;

public class Node {

    private static ConcurentLinkedQueue<Task> completed =
        new ConcurrentLinkedQueue<Task>();

    public static void main(String[] args) {
        if(args.length != 1) {
            String error = "Expects command of the form:\n" +
                "Node <port>\n" +
                "Where <port> is the port it is listening on \n";
            System.out.print(error);
            return;
        }

        int port = Integer.valueOf(args[0]).intValue();
        ServerSocket server = null;
        Socket master = null;
        try {
            server = new ServerSocket(port);
            master = server.accept();
        } catch (Exception e) {
            System.out.println(e);
            return;
        }

        boolean isRunning = true;
        Ping ping = null;
        Pong response = null;
        while(isRunning) {
            try {
                ping = (Ping)Ping.recieve(master);
                switch (ping.command()) {
                    case QUERY: response = new Pong();
                                break;
                    case TASK: break;
                    case RECEIVE: System.out.println("Receiving file "+ ping.filepath());
                                  Ping.recieveFile(master, ping.filepath());
                                  break;
                    case KILL: isRunning = false;
                               break;
                    default: break;
                }
            } catch (IOException e) {
                System.out.println(e);
                return;    
            }
        }
        // Clean up
        try {
            master.close();
        }
        catch (IOException e) {
            // do nothing
        }
    }

// Takes as arguments
// 1) MapClass containing mapping function
// 2) path to input file
// 3) start of record range
// 4) end of record range
// 5) path to output file to append results to

public class Mapper implements Runnable {

    private MapClass mapper;

    // constructor
    public Mapper (MapClass mapper, String infilepath, String outfilepath) {

    // Make hashtable to store mapped values in 
    Hashtable<?, ArrayList<?>> mapped = new Hashtable<?, ArrayList<?>>();

    // Make reader and writer for file

    File outfile = new File(outfilepath);
    ObjectOutputStream out =
	new ObjectOutputStream(new FileOutputStream(outfile));
    BufferedReader in = new BufferedReader(new FileReader(infilepath));
    String input = in.readLine();

    // Read in file and map and write results
    // in the record version, while loop should go to EOF or the end given
  
    while(input != null){
        if(input == null) break;

        Map.Entry<?,?> result = func.map(input);

	// Add key, value to hashtable if not in
	// Append value if it is
	
	if(mapped.get(result.getKey()) == null){
	    ArrayList<?> newval = new ArrayList<?>();
	    newval.add(result.getValue());
	    mapped.put(result.getKey(), newval);
	} 
	else{
	    ArrayList<?> values = mapped.get(result.getKey());
	    values.add(result.getValue());
	    mapped.put(result.getKey(), values);
	}

        input = in.readLine();
    }

    // Write hashtable to file
    out.writeObject(mapped);
    out.flush();

    // Cleanup
    try{
        out.close();
        in.close();
    } catch (Exception e) {
        System.out.println(e);
    }

}
}
public static Reducer implements Runnable{

public void reducer(MapClass func, String infilepath1, String infilepath2, String outfile)
throws FileNotFoundExceptionIOException {

    // Create output file
    File outputfile = new File(outfile);

    // Create streams to files
    ObjectOutputStream out = 
	new ObjectOutputStream(new FileOutputStream(outputfile));

    FileInputStream fs1 = new FileInputStream(infilepath1);
    ObjectInputStream infile1 = new ObjectInputStream(fs1);

    FileInputStream fs2 = new FileInputStream(infilepath2);
    ObjectInputStream infile2 = new ObjectInputStream(fs2);

    // Read in the previously mapped objects
    
    Hashtable<?, ArrayList<?>> map1 = 
	(Hashtable<?, ArrayList<?>>)infile1.readObject();

    Hashtable<?, ArrayList<?>> map2 = 
	(Hashtable<?, ArrayList<?>>)infile2.readObject();

    Hashtable<?, ArrayList<?>> combined = new Hashtable<?, ArrayList<?>>();

    // merge the hashtables

    Enumeration en = map2.keys();
    while(en.hasMoreElements()){
	Object key = en.nextElement();
	ArrayList<?> values = map2.get(key);
	if(map1.contains(key)){
	    values.addAll(map1.get(key));
	}
	combined.put(key, values);
    }

    Iterator<Map.Entry<?, ArrayList<?>>> it = combined.entrySet().iterator();

    // Create final result
    Hashtable<?,?> final = new Hashtable<?,?>();

    while(it.hasNext()){
	Map.Entry<?, ArrayList<?>> entry = it.next();

	Map.Entry<?, ?> reduced = func.reduce(entry);
	final.put(reduced.getKey(), reduced.getValue());
    }

    // Write out final result
    out.writeObject(final);
    out.flush();	

   // Clean up
   try{
	out.close();
	infile1.close();
	infile2.close();

   } catch (Exception e){
	System.out.println(e);
   }

}
}
