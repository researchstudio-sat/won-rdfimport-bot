The rdfimport-bot is a command-line tool for importing data present as RDF into the [Web of Needs](https://github.com/researchstudio-sat/webofneeds/).

The RDF data to be imported must be made available as a set of files in any RDF dialect that can be understood by apache jena.

The bot must be able to make a network connection to at least one WoN node. It is not required that they run on the same machine.

## Configuration
The recommended way of configuring the bot is to copy the `conf` folder to `conf.local` and then make changes in that new folder.

The bot reads files from its [configured](conf/rdfimport-bot.properties) input folder for needs, which are published on the WoN nodes the bot is [configured to use](conf/node-uri-source.properties). Then, the [configured](conf/rdfimport-bot.properties) import folder for connections is read in and for each file, one connection is created. 

With the default configuration, the bot keeps its application state in memory, so it cannot keep state over multiple consecutive runs. Changing the config property `botContext.impl` to `mongoBotContext` in [`bot.properties`](conf/bot.properties)) tells the bot to store its application state in the configured mongo db instance (default: user `won`, password `won`, which needs write access to the database `won`). Doing that will cause the bot to skip creating needs and connections it has already created in an earlier run. 



Checklist of config properties to change:
``` 
bot.properties: 
botContext.impl=mongoBotContext

node-uri-source.properties:
won.node.uris=<your WoN node uris, comma-separated>

owner.properties:
node.default.scheme=<whatever your default WoN node uses, probably http or https>
node.default.host=<host of the default WoN node>
node.default.http.port=<port of the default WoN node, probably 80, 443, 8080, or 8443>

rdfimport-bot.properties:
rdfimport.bot.importfolder.needs=<folder containing your needs as rdf files>
rdfimport.bot.importfolder.connections=<folder containing your connections as rdf files>

```



## Usage

Before building and running the bot, the [Web of Needs](https://github.com/researchstudio-sat/webofneeds/) artifacts have to be in the local maven repository. This is done by checking out that project and [building it once](https://github.com/researchstudio-sat/webofneeds/blob/master/documentation/building-with-maven.md) TL;DR: use this maven command in the webofneeds project directory:
```
mvn install -P skip-frontend,skip-matcher-uberjar,skip-bot-uberjar,skip-matcher-rescal-uberjar,skip-node-webapp-war,skip-owner-webapp-war,skip-tests
``` 

Windows:
```
mvn install 
java -DWON_CONFIG_DIR=conf.local -Dlogback.configurationFile=conf.local/logback.xml -classpath "target/bouncycastle-libs/bcpkix-jdk15on-1.52.jar;target/bouncycastle-libs/bcprov-jdk15on-1.52.jar;target/rdfimport-bot.jar" won.rdfimport.RdfImportBotApp 
```

Unix:
```
mvn install 
java -DWON_CONFIG_DIR=conf.local -Dlogback.configurationFile=conf.local/logback.xml -classpath "target/bouncycastle-libs/bcpkix-jdk15on-1.52.jar:target/bouncycastle-libs/bcprov-jdk15on-1.52.jar:target/rdfimport-bot.jar" won.rdfimport.RdfImportBotApp 
```



## Notes
### So much DEBUG output?
If you are seeing a lot of log output with loglevel DEBUG, you are probably not passing the configuation parameter `-Dlogback.configurationFile` to the bot or it does not point to the right file. 
The top of the log output should help you clarify this:
```
17:08:22,316 |-INFO in ch.qos.logback.classic.LoggerContext[default] - Found resource [conf.local/logback.xml] at [file:/home/won/workspace/won-rdfimport-bot/conf.local/logback.xml]
17:08:22,466 |-INFO in ch.qos.logback.classic.joran.action.ConfigurationAction - Setting ReconfigureOnChangeFilter scanning period to 30 seconds
```
If you don't see such a line at the top, double check the config param you provide. If you do, look into that file, it probably sets the loglevel to DEBUG.


### Why no executable jar?
Unfortunately it is not possible to make one big jar with all dependencies because the signatures on the bouncycastle libraries are only accepted by the JVM if the library are used as separate jar files, therefore the command line is a little more verbose than one might expect. 


