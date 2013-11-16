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

        NodeInfo(Socket socket) {
            this.socket = socket;
            this.tasks = new LinkedList<Task>();
        }
    }

    // Information about the nodes
    private static Hashtable<Integer, NodeInfo> nodes =
        new Hashtable<Integer, NodeInfo>();

    // The distributed file system
    private static DistFileSystem dfs;

    // Assign each task a unique number with tasknum
    private static int jobnum = 0;

    private static class JobInfo {
        public int partnum; // number of partitions for file
        MapClass mapreduce;
        Queue<String> fileparts;
        public int nummerged; // number of files merged so far

        JobInfo(MapClass mapreduce, int partnum) {
            this.mapreduce = mapreduce;
            this.partnum = partnum;
            nummerged = 0;
            fileparts = new ConcurrentLinkedQueue<String>();
        }
    }

    private static Hashtable<Integer, JobInfo> jobs =
        new Hashtable<Integer, JobInfo>();

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

        dfs = new DistFileSystem(nodes.keySet(),scfileprefix,screcordsperfile,
                                 scnumdup);

System.out.println("Prefix: " + scfileprefix + " NumDup: " + scnumdup + " RPF: " + screcordsperfile);

        Heartbeat hb = new Heartbeat();
        Thread hbthread = new Thread(hb);
        hbthread.start();

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
		    System.out.println(dfs.files().toString());
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
                            dfs.distributeFile(filepath, reclen);
                        }

                        // add job to hashtable
                        jobnum++;
                        JobInfo jobinfo = new JobInfo(mapper,
                                                      dfs.splitNum(filepath));
                        jobs.put(jobnum, jobinfo);

                        System.out.println("Processing request...");
                        System.out.println("You are job number: " + jobnum);

                        // Create tasks for mapping on each file partition
                        for(int i = 0; i < dfs.splitNum(filepath); i++) {
                            Task task = new Task(jobnum, Task.Type.MAPRED,
                                mapper, reclen,
                                dfs.fileprefix + filepath + i,
                                dfs.fileprefix + filepath + i + "red");
                            assignTask(task);
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

    private static void assignTask(Task task) {
        Ping ping = new Ping(Ping.Command.TASK, task);
        List<Integer> candidates;

        if (task.infiles().size() == 1) {
            candidates = dfs.listFileLoc(task.infile());
        } else {
            // choose a candidate that already has one of the infiles
            // if possible
            candidates = new ArrayList<Integer>();
            for(String infile : task.infiles()) {
                candidates.addAll(dfs.listFileLoc(infile));
            }
        }
        if(candidates.size() == 0) {
            candidates = new ArrayList<Integer>(nodes.keySet());
        }

        Collections.shuffle(candidates);
        Integer candidate = candidates.get(0);

        // Give candidate all files it doesn't have
        for(String infile : task.infiles()) {
            if(!dfs.hasFile(candidate, infile)) {
                ping = new Ping(Ping.Command.RECEIVE, infile);
                addPing(candidate, ping);
            }
        }
        ping = new Ping(Ping.Command.TASK, task);
        nodes.get(candidate).tasks.add(task);
        addPing(candidate, ping);
    }

    private static void addPing(Integer node, Ping ping) {
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
                Set<Integer> deadnodes = new HashSet<Integer>();
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
                        reply = (Pong)Message.receive(socket);
                        processReply(key, reply);
                    } catch(IOException e) {
                        System.out.println(e);
                        deadnodes.add(key);
                    }
                }
                // Remove dead nodes
                for(Integer corpse : deadnodes) {
                    dfs.remove(corpse);
                    // Reassign tasks
                    for(Task task : nodes.get(corpse).tasks) {
                        assignTask(task);
                    }
                    nodes.remove(corpse);
                }
            }
        }
    }

    private static void processReply(Integer node, Pong reply) throws IOException {
        if(reply.type() == Pong.Type.EMPTY)
            return;
        // Otherwise, it is type TASK
        Task task = reply.task();

        Message.receiveFile(nodes.get(node).socket, task.outfile());
        dfs.add(node, task.outfile());

        // The received task will be a reduce result
        // process it
        JobInfo info = jobs.get(task.jobnum());

        if(task.type() == Task.Type.REDUCE)
            info.nummerged++;
        if(info.nummerged == info.partnum - 1) {
            System.out.println("Task " + task.jobnum() + " is complete!");
            System.out.println("The output is available in file \"" + task.outfile() + "\"");
        } else {
            if(info.fileparts.isEmpty()) {
                info.fileparts.add(task.outfile());
            } else {
                List<String> infiles = new ArrayList<String>(2);
                infiles.add(info.fileparts.remove());
                infiles.add(task.outfile());
                Task newtask = new Task(task.jobnum(), Task.Type.REDUCE,
                    task.mapreduce(), task.recordlen(), infiles,
                    task.outfile() + info.nummerged);
                assignTask(newtask);
            }
            jobs.put(task.jobnum(), info);
        }
    }

    public static class DistFileSystem {

	private int recordsperfile;
	private int numdup;
        // The location where the file is stored.  Usually /tmp/
	private String fileprefix;

        private Hashtable<Integer, List<String>> files =
            new Hashtable<Integer, List<String>>();

        // Keeps track of the number of partitions each file is split into
	private Hashtable<String, Integer> splitfiles =
	    new Hashtable<String, Integer>();

	// Constructor
	DistFileSystem(Collection<Integer> nodes, String fileprefix, int rpf,
                       int nd) {
            for(Integer node : nodes) {
                List<String> filelist = new ArrayList<String>();
                files.put(node, filelist);
            }
	    this.fileprefix = fileprefix;
	    this.recordsperfile = rpf;
	    this.numdup = nd;
	}

	public Hashtable<Integer, List<String>> files() {
	    return files;
        }

	// Assumes 'node' is already in 'nodes'
	public void add (Integer node, String filename) {
	    List<String> filelist = files.get(node);
	    filelist.add(filename);
            files.put(node, filelist);
	}

        public void remove (Integer node) {
            files.remove(node);
        }

        public boolean hasFile(Integer node, String filename) {
            return files.containsKey(node) &&
                   files.get(node).contains(filename);
        }

	public List<Integer> listFileLoc(String filename) {
	    List<Integer> nodelist = new LinkedList<Integer>();

	    for(Integer node : files.keySet()) {
		if(hasFile(node, filename))
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

        private void distributeFile(String filepath, int recsize) throws FileNotFoundException, IOException {
        // split file
        splitFile(filepath, recsize);

        List<Integer> nodelist = new ArrayList<Integer>(nodes.keySet());
        numdup = Math.min(nodelist.size(), numdup);
        // Send split files to ndoes
 for(int i = 0; i < splitNum(filepath); i++) {
            Ping ping = new Ping(Ping.Command.RECEIVE,
                                 fileprefix + filepath + i);
            Collections.shuffle(nodelist);
            for(Integer node : nodelist.subList(0, numdup)) {
                addPing(node, ping);
                add(node, fileprefix + filepath + i);
            }
        }
    }
    }
}
