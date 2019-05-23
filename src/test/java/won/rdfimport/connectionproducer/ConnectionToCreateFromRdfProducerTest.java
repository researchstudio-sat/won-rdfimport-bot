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


import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.Assert;
import org.junit.Test;

public class ConnectionToCreateFromRdfProducerTest{
    @Test
    public void testAtomAndTargetAtomOnly(){
        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, "src/test/resources/connections/set1/42c3083a707fc517f0735ffb71001f50c4f9d89e26f0249080d43441a9e0783e.ttl");
        ConnectionToCreateFromRdfProducer producer = new ConnectionToCreateFromRdfProducer();
        ConnectionToCreate connectionToCreate = producer.makeConnectionToCreate(model);
        Assert.assertEquals("https://node.matchat.org/won/resource/atom/20c9a0395c9688e5a2b158c220bd844c2ddfc34b4bac4fa23bb413affd1268dc",connectionToCreate.getInternalIdFrom().getURI());
        Assert.assertEquals("https://node.matchat.org/won/resource/atom/b605499411b0a915ddee773de321c843bb3a448d754abd76b7d470ed1f55c988",connectionToCreate.getInternalIdTo().getURI());
        Assert.assertNull(connectionToCreate.getState());
        Assert.assertNotNull(connectionToCreate.getFeedback());
        ConnectionToCreate.Feedback feedback = connectionToCreate.getFeedback();
        Assert.assertEquals("http://linked.opendata.cz/resource/isvz.cz/connection/20c9a0395c9688e5a2b158c220bd844c2ddfc34b4bac4fa23bb413affd1268dc", feedback.getInternalIdRated().getURI());
        Assert.assertEquals("https://w3id.org/won/content#binaryRating", feedback.getRatingProperty().getURI());
        Assert.assertEquals("https://w3id.org/won/content#Good", feedback.getRatingValue().asResource().getURI());
    }
}
