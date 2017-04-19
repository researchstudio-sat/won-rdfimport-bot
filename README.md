The rdfimport-bot is a command-line tool for importing data present as RDF into the [Web of Needs](https://github.com/researchstudio-sat/webofneeds/).

The RDF data to be imported must be made available as a set of files in any RDF dialect that can be understood by apache jena.

The bot reads files from its [configured](conf/rdfimport-bot.properties) input folder for needs, which are published on the WoN nodes the bot is [configured to use](conf/node-uri-source.properties). Then, the [configured](conf/rdfimport-bot.properties) import folder for conecitons is read in and for each file, one connection is created. 

The recommended way of configuring the bot is to copy the `conf` folder to `conf.local` and then make changes in that new folder.

The bot also requires a mongodb instance to be running where a user `won` with password `won` has write access to the database `won` (can be configured in [`bot.properties`](conf/bot.properties).


*Usage*:
```
mvn install 
java -DWON_CONFIG_DIR=conf.local -Dlogback.configurationFile=conf.local/logback.xml -classpath "target/bouncycastle-l
ibs/bcpkix-jdk15on-1.52.jar;target/bouncycastle-libs/bcprov-jdk15on-1.52.jar;target/rdfimport-bot.jar" won.rdfimport.RdfImportBotApp 
```

Side note:

Unfortunately it is not possible to make one big jar with all dependencies because the signatures on the bouncycastle libraries are only accepted by the JVM if the library are used as separate jar files, therefore the command line is a little more verbose than one might expect. 


