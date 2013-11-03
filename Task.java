public class Task {
    public enum Type {
        MAP, REDUCE;
    }

    private Type type;
    private MapClass mapreduce;
    private String infile;

    // Constructor
    Task(Type type, MapClass mr, String infile) {
        this.type = type;
	this.mapreduce = mr;
	this.infile = infile;
    }

    // Accessors
    public Type type() {
        return type;
    }

    public MapClass mapreduce() {
        return mapreduce;
    }

    public String infile() {
        return infile;
    }
}
