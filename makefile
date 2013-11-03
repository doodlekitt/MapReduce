JCFLAGS = -g
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
        MapClass.java \
        Task.java \
	Message.java \
        Ping.java \
        Pong.java \
        DistFileSystem.java \
        Master.java \
        Node.java \

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
