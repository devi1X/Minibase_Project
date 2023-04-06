
 

JARFILES=bufmgr/*.class diskmgr/*.class global/*.class iterator/*.class\
         heap/*.class chainexception/*.class  btree/*.class index/*.class tests/*.class

#JDKPATH = /usr/java/jdk1.3.1_02
JDKPATH = /Library/Java/JavaVirtualMachines/jdk-13.0.1.jdk/Contents/Home
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core