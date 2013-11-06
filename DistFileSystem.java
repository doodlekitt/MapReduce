import java.io.*;
import java.util.*;

public class DistFileSystem {

    private HashMap<Integer, List<String>> files =  new HashMap<Integer, List<String>>();

    private int recordsperfile;
    private String relativefilepath;

    DistFileSystem(String rfp, int rpf) {
        this.relativefilepath = rfp;
        this.recordsperfile = rpf;
    }

    public List<Node> ListFileLocations(String filename) {
        return null;
    }

    public void SplitFile(String filepath, int recordsize) throws FileNotFoundException {
        File file = new File(filepath);
    }
}
