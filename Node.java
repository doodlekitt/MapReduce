import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Node {

    private static Queue<Pong> pongs = new ConcurrentLinkedQueue<Pong>();

    private static int availCores; // the number of available cores

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

        // Number of available cores
        // minus 1 for the current thread
        availCores = Runtime.getRuntime().availableProcessors() - 1;

        boolean isRunning = true;
        Ping ping = null;
        Pong response = null;
        while(isRunning) {
            try {
                ping = (Ping)Ping.receive(master);
                switch (ping.command()) {
                    case QUERY: response = new Pong();
                                break;
                    case TASK: Thread newthread = null;
                               if(ping.task().type() == Task.Type.MAPRED) {
                                   newthread = new Thread(new Mapper(ping.task()));
                               } else { // REDUCE
                                   newthread = new Thread(new Reducer(ping.task()));
                               }
                               newthread.start();
                               break;
                    case RECEIVE: System.out.println("Receiving file "+
                                                     ping.filepath());
                                  Ping.receiveFile(master, ping.filepath());
                                  break;
                    case KILL: isRunning = false;
                               break;
                    default: break;
                }
                // Send an update to Master
                Pong pong = null;
                if(pongs.isEmpty()) {
                    pong = new Pong();
                } else {
                    pong = pongs.remove();
                }
                Message.send(master, pong);
                // send the finished product
                if(pong.type() == Pong.Type.TASK && pong.success())
                    Message.sendFile(master, pong.task().outfile());
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

    String outfilepath = task.infile() + "map";

    // Make reader and writer for file
    try {
        File outfile = new File(outfilepath);
        ObjectOutputStream out =
            new ObjectOutputStream(new FileOutputStream(outfile));
        FileInputStream fis = new FileInputStream(task.infile());

    while(fis.available() > 0) {
        // Read input from file
        byte[] bytes = new byte[task.recordlen()];
        fis.read(bytes);
        String input = new String(bytes);

        Map.Entry<Object,Object> result = mapper.map(input);

	// Add key, value to hashtable if not in
	// Append value if it is
	List<Object> values;
	if(!mapped.containsKey(result.getKey())){
	    values = new ArrayList<Object>();
	} 
	else{
	    values = mapped.get(result.getKey());
	}
        values.add(result.getValue());
        mapped.put(result.getKey(), values);
    }

System.out.println("Finished mapping:");
System.out.println(mapped.toString());

    // Write hashtable to file
    out.writeObject(mapped);
    out.flush();

    // Upon success, reduce it
    Task redtask = new Task(task.jobnum(), task.type(), task.mapreduce(),
        task.recordlen(), outfilepath, task.outfile());
    Reducer red = new Reducer(redtask);
    red.run();

    // Cleanup
        out.close();
        fis.close();
    } catch (Exception e) {
        System.out.println(e);
        // Report failure
        Pong pong = new Pong(task, false);
        pongs.add(pong);
    }

}
}

public static class Reducer implements Runnable {

Task task;

Reducer (Task task) {
    this.task = task;
}

public void run() {
    // Create output file
    File outfile = new File(task.outfile());

    // Create streams to files
    List<Hashtable<Object, List<Object>>> tables =
        new ArrayList<Hashtable<Object, List<Object>>>(task.infiles().size());

    for(String infile : task.infiles()) {
        try {
            FileInputStream fis = new FileInputStream(infile);
            ObjectInputStream ois = new ObjectInputStream(fis);
            @SuppressWarnings("unchecked")
            Hashtable<Object, List<Object>> table =
                (Hashtable<Object, List<Object>>)ois.readObject();
            tables.add(table);
            ois.close();
        } catch (Exception e) {
            System.out.println(e);
            // TODO: Add additional error counting?
            return;
        }
    }

    // Merge and reduce the hashtable(s)
    Set<Object> keys = new HashSet<Object>();
    for(Hashtable<Object, List<Object>> table : tables) {
        keys.addAll(table.keySet());
    }

    Hashtable<Object, Object> results = new Hashtable<Object, Object>();

    for(Object key : keys) {
        List<Object> values = new ArrayList<Object>();
        for(Hashtable<Object, List<Object>> table : tables) {
            if(table.containsKey(key)) {
                if(task.type() == Task.Type.MAPRED) {
                    values.addAll(table.get(key));
                } else {
                    values.add(table.get(key));
                }
            }
        }

        // reduce
        Map.Entry<Object, List<Object>> entry =
            new AbstractMap.SimpleEntry<Object, List<Object>>(key, values);
        
        Map.Entry<Object, Object> result = task.mapreduce().reduce(entry);
        results.put(result.getKey(), result.getValue());
    }

System.out.println("Finished reducing:");
System.out.println(results.toString());

    // Write out final result
    try {
    ObjectOutputStream out =
        new ObjectOutputStream(new FileOutputStream(task.outfile()));
    out.writeObject(results);
    out.flush();

    // Report success
    Pong pong = new Pong(task, true);
    pongs.add(pong);

	out.close();
   } catch (Exception e){
	System.out.println(e);
        // Report failure
        Pong pong = new Pong(task, false);
        pongs.add(pong);
   }

}
}

}
