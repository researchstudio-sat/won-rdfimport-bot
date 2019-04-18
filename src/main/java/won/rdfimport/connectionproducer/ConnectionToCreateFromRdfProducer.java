/*
 * Copyright 2012  Research Studios Austria Forschungsges.m.b.H.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package won.rdfimport.connectionproducer;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import won.protocol.model.ConnectionState;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.stream.Collectors;



public class ConnectionToCreateFromRdfProducer {
    private String queryForMainData;

    public ConnectionToCreateFromRdfProducer() {
        init();
    }

    private void init() {
        InputStream inputStream = this.getClass().getResourceAsStream("/sparql/select-connection-data.rq");
        this.queryForMainData = new BufferedReader(new InputStreamReader(inputStream))
                .lines().collect(Collectors.joining("\n"));
    }

    public ConnectionToCreate makeConnectionToCreate(Model model){
        Query query = QueryFactory.create(queryForMainData) ;
        try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
            ResultSet results = qexec.execSelect() ;
            if (!results.hasNext()) throw new IllegalArgumentException("could not extract connection data from specified model");
            QuerySolution soln = results.nextSolution() ;
            Resource atom = soln.getResource("atom") ;
            Resource targetAtom = soln.getResource("targetAtom") ;
            Resource connection = soln.getResource("connection");
            Resource state = null;
            ConnectionState connectionState = null;
            ConnectionToCreate.Feedback feedback = null;
            if (soln.contains("state")){
                state = soln.getResource("state");
                connectionState = ConnectionState.fromURI(URI.create(state.getURI()));
            }
            if (soln.contains("rated")){
                Resource rated = soln.getResource("rated");
                Resource ratingProperty = soln.getResource("ratingProperty");
                RDFNode value = soln.getResource("ratingValue");
                feedback = new ConnectionToCreate.Feedback(rated, ratingProperty, value);
            }
            if (results.hasNext()) throw new IllegalArgumentException("more than one solution found for connection data in specified model");
            return new ConnectionToCreate(atom, targetAtom, connection, connectionState, feedback);
        }
    }
}