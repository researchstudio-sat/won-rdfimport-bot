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

import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import won.protocol.model.ConnectionState;


/**
 * Created by fkleedorfer on 23.03.2017.
 */
public class ConnectionToCreate {
    //the internal id (i.e. not expected to be valid on the Web) of the own atom to connect
    private Resource internalIdFrom;

    //the internal id (i.e. not expected to be valid on the Web) of the 'remote atom to connect to
    private Resource internalIdTo;

    //the intnernal id (i.e. not expected to be valid on the Web) of the connection
    private Resource internalConnectionId;

    // (optional) the state the connection should be brought into
    private ConnectionState state;

    // (optional) The feedback that should be posted to the connection
    private Feedback feedback;

    public ConnectionToCreate(Resource internalIdFrom, Resource internalIdTo, Resource internalConnectionId, ConnectionState state, Feedback feedback) {
        this.internalIdFrom = internalIdFrom;
        this.internalIdTo = internalIdTo;
        this.internalConnectionId = internalConnectionId;
        this.state = state;
        this.feedback = feedback;
    }

    public Resource getInternalIdFrom() {
        return internalIdFrom;
    }

    public Resource getInternalIdTo() {
        return internalIdTo;
    }

    public Resource getInternalConnectionId() {
        return internalConnectionId;
    }

    public ConnectionState getState() {
        return state;
    }

    public Feedback getFeedback() {
        return feedback;
    }

    public static class Feedback
    {
        private Resource internalIdRated;
        private Resource ratingProperty;
        private RDFNode ratingValue;

        public Feedback(Resource internalIdRated, Resource ratingProperty, RDFNode ratingValue) {
            this.internalIdRated = internalIdRated;
            this.ratingProperty = ratingProperty;
            this.ratingValue = ratingValue;
        }

        public Resource getInternalIdRated() {
            return internalIdRated;
        }

        public Resource getRatingProperty() {
            return ratingProperty;
        }

        public RDFNode getRatingValue() {
            return ratingValue;
        }
    }
}
