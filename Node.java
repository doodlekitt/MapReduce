import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Node {

    private static Queue<Task> completed =
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

public static class Mapper implements Runnable {

    Task task;

    // Constructor
    Mapper (Task task) {
        this.task = task;
    }

    public void run() {

    MapClass mapper = task.mapreduce();

    // Make hashtable to store mapped values in 
    Hashtable<Object, List<Object>> mapped =
        new Hashtable<Object, List<Object>>();

    // Make reader and writer for file
    try {
        File outfile = new File(task.outfile());
        ObjectOutputStream out =
            new ObjectOutputStream(new FileOutputStream(outfile));
        FileInputStream fis = new FileInputStream(task.infile());

    while(fis.available() > 0) {
        // Read input from file
        byte[] bytes = new byte[task.recordlen()];
        fis.read(bytes);
        String input = new String(bytes);

        Map.Entry<Object,Object> result =
            (Map.Entry<Object, Object>)mapper.map(input);

	// Add key, value to hashtable if not in
	// Append value if it is
	List<Object> values;
	if(!mapped.contains(result.getKey())){
	    values = new ArrayList<Object>();
	} 
	else{
	    values = mapped.get(result.getKey());
	}
        values.add(result.getValue());
        mapped.put(result.getKey(), values);
    }

    // Write hashtable to file
    out.writeObject(mapped);
    out.flush();

    // Cleanup
        out.close();
        fis.close();
    } catch (Exception e) {
        System.out.println(e);
        // TODO: Additional error handling?
    }

}
}

public static class Reducer implements Runnable{

Task task;

Reducer (Task task) {
    this.task = task;
}

public void run() {
    // Create output file
    File outfile = new File(task.outfile());

    // Create streams to files
    ObjectOutputStream out = 
	new ObjectOutputStream(new FileOutputStream(outfile));

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
*/
}
