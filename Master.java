import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;

public class Master {

    private static class NodeInfo {
        public String host;
        public int port;
        public Socket socket;
        public List<Task> tasks;
        public List<String> files;

        NodeInfo(String host, int port, Socket socket) {
            this.host = host;
            this.port = port;
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
        int i = 0;

        while(sc.hasNext()) {
            try {
                // Discard the first line, as it has info on master
                if(i == 0) {
                    sc.nextLine();
                } else {
                    String hostname = sc.next();
                    int port = sc.nextInt();
                    Socket socket = new Socket(hostname, port);
                    nodes.put(i, new NodeInfo(hostname, port, socket));
                    System.out.println("Added node: " + i + " at host "+hostname+ " and port " + port);
	        }
            } catch (IOException e) {
                System.out.println(e);
            }
            i++;
        }

        // Initialize the Distributed File System
        // Hard coding some of the parameters
        dfs = new DistFileSystem("//tmp//", 100);

// TODO: Remove
try {
dfs.SplitFile("AddInput.txt", 2);
} catch (Exception e) {
System.out.println(e);
return;
}

dfs.CopyFile("AddInput.txt0", 0, 1);

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
		if(command.startsWith("help")){
		    String help = "Available Commands:\n";
		    help += "quit: quit the network\n";
		    help += "list: lists files in distributed file system\n";
		    help += "mapreduce: starts new mapreduce\n";
		    System.out.print(help);
		} else if(command.startsWith("quit")){
		    break;
		} else if(command.startsWith("list")){
		    // TODO

		} else if(command.startsWith("mapreduce")){
		    if(commandargs.length < 4){
			System.out.println("Expecting command of form:");
			System.out.println("mapreduce <MapClass> <recordlength>"
                                           + " <files>");
		    } else{
		    	// Check if the class is valid
			Class<?> c = Class.forName(commandargs[1]);
			
			// Extract record length in bytes
			int reclen = Integer.valueOf(commandargs[2]).intValue();
		
			// Extract file argument array
			String[] files = Arrays.copyOfRange(commandargs, 3, 
					commandargs.length);	

			// Make new MapClass object
			MapClass mapper = (MapClass)c.newInstance();

			// TODO: Make it mapreduce using the given info
		    }
		} else {
		    System.out.print("Invalid command");
		}
	    }

	} catch (Exception e) {
	    System.out.println(e);
	}


    }

    private static class Heartbeat implements Runnable {

        private static boolean isRunning = true;

        public void stop() {
            isRunning = false;
        }

        public void run() {
            Message reply;
            while(isRunning) {
                for(Integer key : nodes.keySet()) {
                    try{
                        Socket socket = nodes.get(key).socket;
                        Message.send(socket, new Ping(Ping.Command.QUERY));
                        reply = (Message)Message.recieve(socket);
                    } catch(IOException e) {
                        System.out.println(e);
                        nodes.remove(key);
                    }
                }
            }
        }
    }

public static class DistFileSystem {

    private int recordsperfile;
    private String relativefilepath;

    DistFileSystem(String rfp, int rpf) {
        this.relativefilepath = rfp;
        this.recordsperfile = rpf;
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

    public void SplitFile(String filepath, int recordsize) throws FileNotFoundException, IOException {
        File infile = new File(filepath);
        FileInputStream fis = new FileInputStream(infile);
        String outfilepath = relativefilepath + filepath;
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
    }

    // takes a filename, a source node (to copy from) and a target node
    // (to copy to)
    // Assumes: filename is located at "relativefilepath"
    // Guarantees: new file will be saved at "relativefilepath"
    public void CopyFile(String filename, Integer source, Integer target) {
	
	String FromPath;
	String ToPath;

        // Set the source file path
	if(source == 0){ // moving from master
            FromPath = relativefilepath + filename;
        } else {
            if(!(nodes.containsKey(source))) {
                System.out.println("Invalid source");
                return;
            }

            NodeInfo src = nodes.get(source);
            Socket sr = src.socket;
            String srcaddr = sr.getRemoteSocketAddress().toString();
            FromPath = "ssh://" + src.host + relativefilepath + filename;
        }

        // Set the target file path
        if(target == 0) { // moving to master
            ToPath = relativefilepath + filename;
        } else {
            if(!(nodes.containsKey(target))) {
            	System.out.println("Invalid target");
            	return;
            }

	    NodeInfo tgt = nodes.get(target);
	    Socket tg = tgt.socket;
	    String tgtaddr = tg.getRemoteSocketAddress().toString();	
	    ToPath = "ssh://" + tgt.host + relativefilepath + filename;
	}

System.out.println("From Path: " + FromPath);
System.out.println("To Path: " + ToPath);

	try{
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
}
}
