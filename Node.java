import java.io.*;
import java.lang.*;
import java.net.*;
import java.util.*;

public class Node {

    public static void main(String[] args) {
        if(args.length != 1) {
            String error = "Expects command of the form:\n" +
                "Master <filename>\n" +
                "Where <filename> contains information on the Nodes\n";
            System.out.print(error);
            return;
        }

        Scanner sc = null;
        Socket master = null;
        try {
            sc = new Scanner(new File(args[1]));
            String hostname = sc.next();
            int port = sc.nextInt();
            master = new Socket(hostname, port);
        } catch (Exception e) {
            System.out.println(e);
            return;
        }

    }

// Takes as arguments
// 1) MapClass containing mapping function
// 2) path to input file
// 3) start of record range
// 4) end of record range
// 5) path to output file to append results to

// SYNCHRONIZE?
public void mapper(MapClass func, String infilepath, int start, int end,
                   String outfilepath) throws FileNotFoundException, IOException
{
    // Make reader and writer for file
    File outfile = new File(outfilepath);
    ObjectOutputStream out =
	new ObjectOutputStream(new FileOutputStream(outfile));
    BufferedReader in = new BufferedReader(new FileReader(infilepath));
    String input = in.readLine();

    // Read in file and map and write results
    // in the record version, while loop should go to EOF or the end given
    while(input != null){
        if(input == null) break;

        String result = func.map(input);
        out.writeObject(result);
        out.flush();
        input = in.readLine();
    }

    // Cleanup
    try{
        out.close();
        in.close();
    } catch (Exception e) {
        System.out.println(e);
    }

}


}
