import java.io.Serializable;

public class Task {
    public enum Type {
        MAP, REDUCE;
    }

    private Type type;
    private MapClass mapreduce;
    private int recordlen;
    private String infile;
    private String outfile;

    // Constructor
    Task(Type type, MapClass mr, int recordlen, String infile, String outfile) {
        this.type = type;
	this.mapreduce = mr;
        this.recordlen = recordlen;
	this.infile = infile;
        this.outfile = outfile;
    }

    // Accessors
    public Type type() {
        return type;
    }

    public MapClass mapreduce() {
        return mapreduce;
    }

    public int recordlen() {
        return recordlen;
    }

    public String infile() {
        return infile;
    }

    public String outfile() {
        return outfile;
    }
}
