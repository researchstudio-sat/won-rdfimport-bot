The rdfimport-bot is a command-line tool for importing data present as RDF into the [Web of Needs](https://github.com/researchstudio-sat/webofneeds/).

The RDF data to be imported must be made available as a set of files in any RDF dialect that can be understood by apache jena.

The bot reads files from its [configured](conf/rdfimport-bot.properties) input folder into an in-memory rdf store and then processess the data in order to produce individual needs, which are published on the WoN nodes the bot is [configured to use](conf/node-uri-source.properties).
The import folder defaults to `./rdfimport`

*Usage*:
```
mvn exec -DmainClass=won.rdfimport.app.RdfImportBotApp 
```



