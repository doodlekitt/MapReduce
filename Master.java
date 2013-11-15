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

        int n = 1;
        while(sc.hasNext()) {
            try {
                String hostname = sc.next();
                int port = sc.nextInt();
                Socket socket = new Socket(hostname, port);
                nodes.put(n, new NodeInfo(socket));
                System.out.println("Added node: " + n + " at host "+hostname+ " and port " + port);
            } catch (IOException e) {
                System.out.println(e);
            }
            n++;
        }

        // Initialize pings
        for(Integer i : nodes.keySet()) {
            Queue<Ping> queue = new ConcurrentLinkedQueue<Ping>();
            pings.put(i, queue);
        }

        // Initialize the Distributed File System
        scnumdup = Math.min(scnumdup, nodes.size());

        dfs = new DistFileSystem(scfileprefix, screcordsperfile, scnumdup);

System.out.println("Prefix: " + scfileprefix + " NumDup: " + scnumdup + " RPF: " + screcordsperfile);

        Heartbeat hb = new Heartbeat();
        Thread hbthread = new Thread(hb);
        hbthread.start();

// TODO: Remove
/*
  try {
dfs.splitFile("AddInput.txt", 2);

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
                        try {
		    	// Check if the class is valid
			Class<?> c = Class.forName(commandargs[1]);
                        MapClass mapper = (MapClass)c.newInstance();
			// Extract record length in bytes
			int reclen = Integer.valueOf(commandargs[2]).intValue();
			// Extract file argument array
			String filepath = commandargs[3];

                        // split the file and distribute the file to nodes
                        // if not already done for this file
                        if(dfs.splitNum(filepath) < 0) {
                            distributeFile(filepath, reclen);
                        }

                        // Create tasks for mapping on each file partition
                        for(int i = 0; i < dfs.splitNum(filepath); i++) {
                            Task task = new Task(Task.Type.MAP, mapper, reclen,
                                dfs.fileprefix + filepath + i,
                                dfs.fileprefix + filepath + i + "map");
                            Ping ping = new Ping(Ping.Command.TASK, task);

                            List<Integer> candidates =
                                dfs.listFileLoc(dfs.fileprefix + filepath + i);
                            Collections.shuffle(candidates);
                            // Assumes that candidate.size() > 0
                            addPing(candidates.get(0), ping);
                        }
                        } catch (Exception e) {
                            System.out.println("Sorry, cannot mapreduce this, exception:");
                            System.out.println(e);
                        }
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

    private static void distributeFile(String filepath, int recsize) throws FileNotFoundException, IOException {
        // split file
        System.out.println("Splitting file " + filepath + " with recsize " + recsize);
	dfs.splitFile(filepath, recsize);

        System.out.println("Filepath = " + filepath);
        System.out.println("Split num = " + dfs.splitNum(filepath));
        System.out.println("Distributing file...");
        List<Integer> nodelist = new ArrayList<Integer>(nodes.keySet());
	dfs.numdup = Math.min(nodelist.size(), dfs.numdup);
        // Send split files to ndoes
	for(int i = 0; i < dfs.splitNum(filepath); i++) {
            Ping ping = new Ping(Ping.Command.RECEIVE,
			         dfs.fileprefix + filepath + i);
	    Collections.shuffle(nodelist);
            for(Integer node : nodelist.subList(0, dfs.numdup)) {
                addPing(node, ping);
                dfs.add(node, dfs.fileprefix + filepath + i);
            }
	}
    }

    private static void addPing(Integer node, Ping ping) {
System.out.println("Adding ping " + ping.command() + " for Node " + node);
        Queue<Ping> queue = pings.get(node);
        queue.add(ping);
        pings.put(node, queue);
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
// TODO: Remove?
if(ping.command() != Ping.Command.QUERY) {
System.out.println("Sending ping " + ping.command() + "to Node " + key);
if(ping.command() == Ping.Command.TASK) {
System.out.println("Task: " + ping.task().type() + " " + ping.task().infile());
}
if(ping.command() == Ping.Command.RECEIVE) {
System.out.println("Sending file" + ping.filepath());
}
}
                        Message.send(socket, ping);
                        if(ping.command() == Ping.Command.RECEIVE) {
                            Message.sendFile(socket, ping.filepath());
                        }
                        reply = (Pong)Message.recieve(socket);
                        processReply(key, reply);
                    } catch(IOException e) {
                        System.out.println(e);
                        // Causes concurrency error
                        // nodes.remove(key);
                    }
                }
            }
        }
    }

    private static void processReply(Integer node, Pong reply) {
        if(reply.type() == Pong.Type.EMPTY)
            return;
        // Otherwise, it is type TASK
        Task task = reply.task();
        if(!reply.success()) {
            // TODO: reassign task
        } else {
            // Add new file to dfs
            dfs.add(node, task.outfile());
            switch(task.type()) {
                case MAP: break;
                case REDUCE: break;
                default: break;
            }
        }
    }

    public static class DistFileSystem {

	private int recordsperfile;
	public int numdup;
	public String fileprefix;
	private Hashtable<String, Integer> splitfiles =
	    new Hashtable<String, Integer>();

	DistFileSystem(String fileprefix, int rpf, int nd) {
	    this.fileprefix = fileprefix;
	    this.recordsperfile = rpf;
	    this.numdup = nd;
	}

	// Assumes 'node' is already in 'nodes'
	public void add (Integer node, String filename) {
	    NodeInfo info = nodes.get(node);
	    info.files.add(filename);
	}

	public List<Integer> listFileLoc(String filename) {
	    List<Integer> nodelist = new LinkedList<Integer>();

	    for(Integer node : nodes.keySet()) {
		if(nodes.get(node).files.contains(filename))
		    nodelist.add(node);
	    }
	    return nodelist;
	}

	// Splits the given file
	// Returns: The number of files split into
	public int splitFile(String filepath, int recordsize) throws FileNotFoundException, IOException {
	    File infile = new File(filepath);
	    FileInputStream fis = new FileInputStream(infile);
	    String outfilepath = fileprefix + filepath;
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

	    splitfiles.put(filepath, i);

	    return i;
	}

	// returns the number of files a file was parititioned into
	// or -1 if it hasn't been split yet
	public int splitNum(String file) {
	    if(!splitfiles.containsKey(file)) {
		return -1;
	    }
	    return (splitfiles.get(file)).intValue();
	}
    }
}
