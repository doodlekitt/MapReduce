public class Ping extends Message {
    public enum Command {
        QUERY, MAP, REDUCE;
    }

    private Command command;
    private MapClass mapper;
    private String filepath;

    // Constructors
    // For QUERY:
    Ping(Command command) {
        this.command = command;
    }

    // For MAP
    Ping(Command command, MapClass mapper, String filepath) {
        this.command = command;
        this.mapper = mapper;
        this.filepath = filepath;
    }
}
