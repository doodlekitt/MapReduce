import java.util.ArrayList;
import java.util.Collection;

public class Pong extends Message {
    private Collection<Task> completed;

    // Constructors
    Pong() {
        this.completed = new ArrayList<Task>();
    }

    Pong(Collection<Task> completed) {
        this.completed = completed;
    }

    // Accessors
    public Collection<Task> completed() {
        return completed;
    }
}
