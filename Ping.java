public class Ping extends Message {
    public enum Command {
        QUERY, MAP, REDUCE;
    }

    private Command command;
    private Task task;

    // Constructors
    // For QUERY:
    Ping(Command command) {
        this.command = command;
    }

    // For map, reduce
    Ping(Command command, Task task) {
        this.command = command;
        this.task = task;
    }
}
