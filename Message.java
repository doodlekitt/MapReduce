import java.io.*;
import java.net.Socket;

public class Message  implements Serializable {

    public static void send(Object message, Socket socket) throws IOException {
        ObjectOutputStream os =
            new ObjectOutputStream(socket.getOutputStream());
        os.flush();
        os.writeObject(message);
    }

    public static Object recieve(Socket socket) throws IOException {
        Object response = null;

        try {
            ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
            response = (Object)is.readObject();
        } catch (ClassNotFoundException e) {
            System.out.println(e);
        }

        return response;
    }
}
