import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;

public class Master {

    private static Hashtable<Integer, Socket> nodes =
        new Hashtable<Integer, Socket>();

    public static void main(String[] args) {
        if(args.length != 1) {
            String error = "Expects command of the form:\n" +
                "Master <filename>\n" +
                "Where <filename> contains information on the Nodes\n";
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
                    nodes.put(i, socket);
	        }
            } catch (IOException e) {
                System.out.println(e);
            }
            i++;
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
                        Socket socket = nodes.get(key);
                        Message.send(socket, new Message());
                        reply = (Message)Message.recieve(socket);
                    } catch(IOException e) {
                        System.out.println(e);
                        nodes.remove(key);
                    }
                }
            }
        }
    }
}
