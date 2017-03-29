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


import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;


public class DirectoryBasedConnectionToCreateProducerTest {
    @Test
    public void testNormalUse(){
        DirectoryBasedConnectionToCreateProducer producer = new DirectoryBasedConnectionToCreateProducer();
        producer.setDirectory(new File("src/test/resources/connections/set1"));
        producer.setFilenameFilterRegex("^[^\\.].+\\.ttl");
        Iterator<ConnectionToCreate> connectionToCreateIterator = producer.getConnectionIterator();
        Assert.assertEquals(3, Iterators.advance(connectionToCreateIterator,3));
    }

}
