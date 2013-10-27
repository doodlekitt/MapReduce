import java.lang.*;
import java.io.*;


public class Node {

// Takes as arguments
// 1) MapClass containing mapping function
// 2) path to input file
// 3) start of record range
// 4) end of record range
// 5) path to output file to append results to

// SYNCHRONIZE?
public void mapper(MapClass func, String infile, int start, int end, 
			String outfile)
{
    // Make reader and writer for file
    // will probably need to change to make allowances for records

    PrintWriter out = new PrintWriter(outfile);
    // call flush here or somewhere else? Needed?

    BufferedReader in = new BufferedReader(new FileReader(infile));

    // Read in file and map and write results
    // in the record version, while loop should go to EOF or the end given
    while(true){
	String input = in.readLine();
	if(input == null) break;

	String result = func.map(input);
	out.println(result);
	out.flush; // correct?
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
