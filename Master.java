import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;

public class Master {
    private static Hashtable<Integer, Socket> nodes =
        new Hashtable<Integer, Socket>();

    public static void main(String[] args) {

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
