import java.io.*;
import java.util.*;

public class DistFileSystem {

    private HashMap<Integer, List<String>> filemap =  new HashMap<Integer, List<String>>();

    private int recordsperfile;
    private String relativefilepath;

    DistFileSystem(String rfp, int rpf) {
        this.relativefilepath = rfp;
        this.recordsperfile = rpf;
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
