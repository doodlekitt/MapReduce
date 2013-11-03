JCFLAGS = -g
JC = javac
.SUFFIXES: .java .class
.java.class:
	$(JC) $(JFLAGS) $*.java

CLASSES = \
	Message.java \
        Ping.java \
        Master.java \
        Node.java \
	MapClass.java \

default: classes

classes: $(CLASSES:.java=.class)

clean:
	$(RM) *.class
