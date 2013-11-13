import java.io.*;
import java.net.Socket;

public class Message implements Serializable {

    public static void send(Socket socket, Object message) throws IOException {
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

    // Sends the content of a local file line by line
    public static void sendFile(Socket socket, String filepath) throws IOException, FileNotFoundException {
        // Read contents of file
        System.out.println("Creating infile and streams...");
        File file = new File(filepath);
        FileInputStream fis = new FileInputStream(file);
        // Assumes that all files are <= 2^32 bytes
        byte[] bytes = new byte[(int)file.length()];
        System.out.println("Reading...");
        fis.read(bytes);

        // Send
        System.out.println("Sending...");
        send(socket, bytes);

        fis.close();
        System.out.println("Done!");
    }

    // Reiceves the content of a file, line by line, and saves it locally
    public static void recieveFile(Socket socket, String filepath) throws IOException, FileNotFoundException {
        System.out.println("Creating outfile and streams...");
        File file = new File(filepath);
        FileOutputStream fos = new FileOutputStream(file);
        fos.flush();

        // Recieve contents of file
        System.out.println("Receiving...");
        byte[] bytes = (byte[])recieve(socket);
        System.out.println("Received");

        System.out.println("Writing...");
        fos.write(bytes);

        fos.flush();
        fos.close();
        System.out.println("Done!");
    }
}
