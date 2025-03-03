JDKPATH = /usr
LIBPATH = lib/bufmgrAssign.jar

CLASSPATH = .:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac 
JAVA  = $(JDKPATH)/bin/java 

PROGS = xx
#PROGS = run

all: $(PROGS)

compile:src/*/*.java
	$(JAVAC) -cp $(CLASSPATH) -d bin src/*/*.java

xx : compile
	$(JAVA) -cp $(CLASSPATH):bin tests.BMTest

# run : compile
# 	$(JAVA) -cp $(CLASSPATH):bin tests.BMTest

clean:
	rm -rf bin/*.class
