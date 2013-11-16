import java.io.*;
import java.net.Socket;

public class Message implements Serializable {

    public static void send(Socket socket, Object message) throws IOException {
        ObjectOutputStream os =
            new ObjectOutputStream(socket.getOutputStream());
        os.flush();
        os.writeObject(message);
    }

    public static Object receive(Socket socket) throws IOException {
        Object response = null;

        try {
            ObjectInputStream is = new ObjectInputStream(socket.getInputStream());
            response = (Object)is.readObject();
        } catch (ClassNotFoundException e) {
            System.out.println(e);
        }

        return response;
    }

    // Sends the content of a local file line by line
    public static void sendFile(Socket socket, String filepath) throws IOException, FileNotFoundException {
        // Read contents of file
        File file = new File(filepath);
        FileInputStream fis = new FileInputStream(file);
        // Assumes that all files are <= 2^32 bytes
        byte[] bytes = new byte[(int)file.length()];
        fis.read(bytes);

        // Send
        send(socket, bytes);

        fis.close();
    }

    // Reiceves the content of a file, line by line, and saves it locally
    public static void receiveFile(Socket socket, String filepath) throws IOException, FileNotFoundException {
        File file = new File(filepath);
        FileOutputStream fos = new FileOutputStream(file);
        fos.flush();

        // Recieve contents of file
        byte[] bytes = (byte[])receive(socket);

        fos.write(bytes);

        fos.flush();
        fos.close();
    }
}
