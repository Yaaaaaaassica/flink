find ./ -type f -regex ".*\.xml" | xargs perl -pi -e"s/2019101/20191101/g"
![state](MDZ/state.md)
![flink-rpc](MDZ/flink-rpc.md)
![stream](MDZ/stream.md)



[flink线程模型](http://myclusterbox.com/view/661)



/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home/bin/java -Dmaven.multiModuleProjectDirectory=/Users/spafka/Desktop/flink -Dmaven.home=/usr/local/apache-maven-3.6.2 -Dclassworlds.conf=/usr/local/apache-maven-3.6.2/bin/m2.conf "-Dmaven.ext.class.path=/Applications/IntelliJ IDEA.app/Contents/plugins/maven/lib/maven-event-listener.jar" "-javaagent:/Applications/IntelliJ IDEA.app/Contents/lib/idea_rt.jar=50589:/Applications/IntelliJ IDEA.app/Contents/bin" -Dfile.encoding=UTF-8 -classpath /usr/local/apache-maven-3.6.2/boot/plexus-classworlds-2.6.0.jar org.codehaus.classworlds.Launcher -Didea.version2019.2.4 --offline -DskipTests=true clean -P skip-webui-build,skip-hive-tests,fast
[
