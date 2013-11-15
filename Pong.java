import java.util.ArrayList;
import java.util.Collection;

public class Pong extends Message {
    public enum Type {
        EMPTY, TASK;
    }

    private Type type;
    private Task task;
    private boolean success;

    // Constructors
    // For EMPTY
    Pong() {
        this.type = Type.EMPTY;
    }

    // For TYPE
    Pong(Task task, boolean success) {
        this.type = Type.TASK;
        this.task = task;
        this.success = success;
    }

    // Accessors
    public Type type() {
        return type;
    }

    public Task task() {
        return task;
    }

    public boolean success() {
        return success;
    }
}
