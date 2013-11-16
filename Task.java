import java.util.List;
import java.util.ArrayList;

public class Task implements java.io.Serializable {
    public enum Type {
        MAPRED, REDUCE;
    }

    private int jobnum;
    private Type type;
    private MapClass mapreduce;
    private int recordlen;
    private List<String> infiles;
    private String outfile;

    // Constructor
    // For MAPRED (on one file)
    Task(int jobnum, Type type, MapClass mr, int recordlen, String infile,
         String outfile) {
        this.jobnum = jobnum;
        this.type = type;
	this.mapreduce = mr;
        this.recordlen = recordlen;
	this.infiles = new ArrayList<String>();
        this.infiles.add(infile);
        this.outfile = outfile;
    }

    // For REDUCE on multiple files
    Task(int jobnum, Type type, MapClass mr, int recordlen, List<String> infile,
         String outfile) {
        this.jobnum = jobnum;
        this.type = type;
        this.mapreduce = mr;
        this.recordlen = recordlen;
        this.infiles = infiles;
        this.outfile = outfile;
    }

    // Accessors
    public int jobnum() {
        return jobnum;
    }

    public Type type() {
        return type;
    }

    public MapClass mapreduce() {
        return mapreduce;
    }

    public int recordlen() {
        return recordlen;
    }

    // For MAPRED
    // Assumes only one infile is provided
    public String infile() {
        return infiles.get(0);
    }

    // For REDUCE
    public List<String> infiles() {
        return infiles;
    }

    public String outfile() {
        return outfile;
    }
}
