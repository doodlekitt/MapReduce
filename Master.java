import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Master {

    private static Hashtable<Integer, Queue<Ping>> pings =
        new Hashtable<Integer, Queue<Ping>>();

    private static class NodeInfo {
        public Socket socket;
        public List<Task> tasks;
        public List<String> files;

        NodeInfo(Socket socket) {
            this.socket = socket;
            this.tasks = new LinkedList<Task>();
            this.files = new LinkedList<String>();
        }
    }

    // Information about the nodes
    private static Hashtable<Integer, NodeInfo> nodes =
        new Hashtable<Integer, NodeInfo>();

    // The distributed file system
    private static DistFileSystem dfs;

    public static void main(String[] args) {
        if(args.length != 1) {
            String error = "Expects command of the form:\n" +
                "Master <filename>\n" +
                "Where <filename> is the config file\n";
            System.out.print(error);
            return;
        }

        // Parse input file
        // Assumes that all nodes are already running and listening for master
        Scanner sc = null;
        try {
            sc = new Scanner(new File(args[0]));
        } catch (FileNotFoundException e) {
            System.out.println(e);
            return;
        }

        // Reads settings from file
        String scfileprefix = sc.next(); // Location to save files for dfs
        int scnumdup = sc.nextInt(); // Number of times to duplicate each file
        int screcordsperfile = sc.nextInt(); // Records per file

        while(sc.hasNext()) {
            int i = 1;
            try {
                // Discard the first line, as it has info on master
                String hostname = sc.next();
                int port = sc.nextInt();
                Socket socket = new Socket(hostname, port);
                nodes.put(i, new NodeInfo(socket));
                System.out.println("Added node: " + i + " at host "+hostname+ " and port " + port);
            } catch (IOException e) {
                System.out.println(e);
            }
            i++;
        }

        // Initialize pings
        for(Integer i : nodes.keySet()) {
            Queue<Ping> queue = new ConcurrentLinkedQueue<Ping>();
            pings.put(i, queue);
        }   

        // Initialize the Distributed File System
        scnumdup = Math.min(scnumdup, nodes.size());

        dfs = new DistFileSystem(scfileprefix, scnumdup, screcordsperfile);

        Heartbeat hb = new Heartbeat();
        Thread hbthread = new Thread(hb);
        hbthread.start();
        
// TODO: Remove
/*
try {
dfs.SplitFile("AddInput.txt", 2);

NodeInfo node = nodes.get(1);

String testfp = dfs.fileprefix + "AddInput.txt0";
System.out.println("Sending file " + testfp);

Ping ping = new Ping(Ping.Command.RECEIVE, testfp);

Message.send(node.socket, ping);
Message.sendFile(node.socket, testfp);

} catch (Exception e) {
System.out.println(e);
return;
}
*/
	// Listen to user commands
	try{
	    BufferedReader br = 
		new BufferedReader(new InputStreamReader(System.in));
	    String command = null;
	    String[] commandargs = null;
	    while(true){
		System.out.print(" > ");
		command = br.readLine();

		commandargs = command.split(" ");
		if(command.startsWith("help")) {
		    String help = "Available Commands:\n";
		    help += "quit: quit the network\n";
		    help += "list: lists files in distributed file system\n";
		    help += "mapreduce: starts new mapreduce\n";
		    System.out.print(help);
		} else if(command.startsWith("quit")) {
		    break;
		} else if(command.startsWith("list")) {
		    // TODO

		} else if(command.startsWith("mapreduce")) {
		    if(commandargs.length != 4) {
			System.out.println("Expecting command of form:");
			System.out.println("mapreduce <MapClass> <recordlength>"
                                           + " <file>");
		    } else {
		    	// Check if the class is valid
			Class<?> c = Class.forName(commandargs[1]);
			
			// Extract record length in bytes
			int recsize = Integer.valueOf(commandargs[2]).intValue();
		
			// Extract file argument array
			String filepath = dfs.fileprefix + commandargs[3];

                        File file = new File(filepath + '0');
                        if(!file.exists()) {
                            System.out.println("Splitting file...");
                            dfs.SplitFile(filepath, recsize);
                        }

                        // Make new MapClass object
                        MapClass mapper = (MapClass)c.newInstance();

                        // Create tasks for mapping on each file partition
                        List<Task> tasks = new ArrayList<Task>();
                        int i = 0;
                        while(file.exists()) {
                            Task task = new Task(Task.Type.MAP, mapper, recsize, filepath + i, filepath + i + "map");
                            tasks.add(task);
                            i++;
                            file = new File(filepath + i);
                        }

                        DistributeTasks(tasks);
		    }
		} else {
		    System.out.print("Invalid command");
		}
	    }

	} catch (Exception e) {
	    System.out.println(e);
	}

        // Clean up
        hb.stop();
    }

    private static void DistributeTasks(List<Task> tasks) {
         return;
    }

    private static class Heartbeat implements Runnable {

        private static boolean isRunning = true;

        public void stop() {
            isRunning = false;
            // Kill all nodes
            for(Integer key : nodes.keySet()) {
                try {
                    Socket socket = nodes.get(key).socket;
                    Message.send(socket, new Ping(Ping.Command.KILL));
                } catch(IOException e) {
                    // do nothing
                }
            }
        }

        public void run() {
            Pong reply = null;
            while(isRunning) {
                for(Integer key : nodes.keySet()) {
                    try{
                        Socket socket = nodes.get(key).socket;
                        Ping ping = null;
                        if(!pings.get(key).isEmpty()) {
                            ping = pings.get(key).remove();
                        } else {
                            ping = new Ping(Ping.Command.QUERY);
                        }
                        Message.send(socket, ping);
                        if(ping.command() == Ping.Command.RECEIVE) {
                            Message.sendFile(socket, ping.filepath());
                        }
                        reply = (Pong)Message.recieve(socket);
                        processReply(reply);
                    } catch(IOException e) {
                        System.out.println(e);
                        nodes.remove(key);
                    }
                }
            }
        }
    }

private static void processReply(Pong reply) {
    return;
}

public static class DistFileSystem {

    private int recordsperfile;
    private int numdup;

    public String fileprefix;

    DistFileSystem(String fileprefix, int rpf, int nd) {
        this.fileprefix = fileprefix;
        this.recordsperfile = rpf;
        this.numdup = nd;
    }

    // Assumes 'node' is already in 'nodes'
    public void Add (Integer node, String filename) {
        NodeInfo info = nodes.get(node);
        info.files.add(filename);
    }

    public List<Integer> ListFileLocations(String filename) {
        List<Integer> nodelist = new LinkedList<Integer>();

        for(Integer node : nodes.keySet()) {
            if(nodes.get(node).files.contains(filename))
                nodelist.add(node);
        }
        return nodelist;
    }

    // Splits the given file
    // Returns: The number of files split into
    public int SplitFile(String filepath, int recordsize) throws FileNotFoundException, IOException {
        File infile = new File(filepath);
        FileInputStream fis = new FileInputStream(infile);
        String outfilepath = filepath + filepath;
        int i = 0;
        while(fis.available() > 0) {
            byte[] bytes = new byte[recordsize * recordsperfile];
            fis.read(bytes);
            File outfile = new File(outfilepath + i);
            FileOutputStream fos = new FileOutputStream(outfile);
            fos.write(bytes);
            fos.close();
            i++;
        }
        fis.close();

        return i;
    }
/*
 * TODO: Remove
	    File newF = new File(ToPath);
	    FileOutputStream fout = new FileOutputStream(ToPath, true);
	    fout.flush();
	    FileInputStream fin = new FileInputStream(FromPath);
	    BufferedReader br = new BufferedReader(new InputStreamReader(fin));
	    PrintStream out = new PrintStream(fout);

            String line = null;
            while(true){
                line = br.readLine();
                if(line == null) break;
                else
                    out.println(line);
            }

	} catch (Exception e) {
	    System.out.println(e);
	}
    }
*/
}
}
