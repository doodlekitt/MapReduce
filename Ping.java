public class Ping extends Message {
    public enum Command {
        QUERY, TASK, RECEIVE;
    }

    private Command command;
    private Task task;
    private String filepath;

    // Constructors
    // For QUERY:
    Ping(Command command) {
        this.command = command;
    }

    // For TASK
    Ping(Command command, Task task) {
        this.command = command;
        this.task = task;
    }

    // For RECEIVE
    Ping(Command command, String filepath) {
        this.command = command;
        this.filepath = filepath;
    }

    Command command() {
        return command;
    }

    Task task() {
        return task;
    }

    String filepath() {
        return filepath;
    }
}
