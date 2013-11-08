import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;

public class Master {

    public enum Status {
        FREE, MAP, REDUCE;
    }

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
            sc = new Scanner(new File(args[1]));
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
                    nodes.put(i, new NodeInfo(socket));
	        }
            } catch (IOException e) {
                System.out.println(e);
            }
            i++;
        }

        // Initialize the Distributed File System
        // Hard coding some of the parameters
        dfs = new DistFileSystem(nodes.keySet(), "/tmp", 100);

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

public class DistFileSystem {

    private HashMap<Integer, List<String>> filemap =  new HashMap<Integer, List<String>>();

    private int recordsperfile;
    private String relativefilepath;

    DistFileSystem(Collection<Integer> nodes, String rfp, int rpf) {
        this.relativefilepath = rfp;
        this.recordsperfile = rpf;
        List<String> files;
        for(Integer node : nodes) {
            files = new LinkedList<String>();
            filemap.put(node, files);
        }
    }

    public void Add (Integer node, String filename) {
        List<String> files;
        if(!filemap.containsKey(node)) {
            files = new LinkedList<String>();
            files.add(filename);
            filemap.put(node, files);
        } else {
            files = filemap.get(node);
            files.add(filename);
            filemap.put(node, files);
        }
    }

    public List<Integer> ListFileLocations(String filename) {
        List<Integer> nodes = new LinkedList<Integer>();

        for(Integer node : filemap.keySet()) {
            if(filemap.get(node).contains(filename))
                nodes.add(node);
        }
        return nodes;
    }

    public void SplitFile(String filepath, int recordsize) throws FileNotFoundException, IOException {
        File infile = new File(filepath);
        FileInputStream fis = new FileInputStream(infile);
        String outfilepath = relativefilepath + filepath;
        int i = 0;
        while(fis.available() > 0) {
            byte[] bytes = new byte[recordsize * recordsperfile];
            File outfile = new File(outfilepath + i);
            FileOutputStream fos = new FileOutputStream(outfile);
            fos.write(bytes);
            fos.close();
            i++;
        }
        fis.close();
    }
}

    // takes a filename, a source node (to copy from) and a target node
    // (to copy to)
    // Assumes: filename is located at "relativefilepath"
    // Guarantees: new file will be saved at "relativefilepath"
    public void CopyFile(String filename, Integer source, Integer target) {
	NodeInfo src = nodes.get(source);
	NodeInfo tgt = nodes.get(target); 

	// Check valid network nodes
	if(src == null || tgt == null){
	    System.out.println("Invalid source or target");
	    break;
	}
	Socket sr = src.socket;
	Socket tg = tgt.socket;
	String srcaddr = sr.getRemoteSocketAddress().toString();
	String tgtaddr = tg.getRemoteSocketAddress().toString();

	String FromPath = "//" + srcaddr + "//" + relativefilepath + filename; 
	String ToPath = "//" + tgtaddr + "//"+ relativefilepath+filename;

	try{
	    File newF = new File(ToPath);
	    FileOutputStream fout = new FileOutputStream(ToPath, true);
	    fout.flush();
	    FileInputStream fin = new FileInputStream(FromPath);
	    PrintStream out = new PrintStream(fout);
	} catch (Exception e) {
	    System.out.println(e);
	}

	String line = null;
	while(true){
	    line = fin.readLine();
	    if(line == null) break;
	    else
	    	out.println(line);
	}

    }

}
