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

package won.rdfimport;

import won.bot.framework.bot.base.EventBot;
import won.bot.framework.eventbot.EventListenerContext;
import won.bot.framework.eventbot.action.BaseEventBotAction;
import won.bot.framework.eventbot.action.impl.MultipleActions;
import won.bot.framework.eventbot.action.impl.PublishEventAction;
import won.bot.framework.eventbot.action.impl.counter.*;
import won.bot.framework.eventbot.action.impl.listener.UnsubscribeListenerAction;
import won.bot.framework.eventbot.action.impl.needlifecycle.CreateNeedWithFacetsAction;
import won.bot.framework.eventbot.action.impl.trigger.*;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.ExecuteConnectCommandAction;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.ExecuteFeedbackCommandAction;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.ExecuteOpenCommandAction;
import won.bot.framework.eventbot.bus.EventBus;
import won.bot.framework.eventbot.event.Event;
import won.bot.framework.eventbot.event.NeedCreationFailedEvent;
import won.bot.framework.eventbot.event.impl.command.connect.ConnectCommandEvent;
import won.bot.framework.eventbot.event.impl.command.connect.ConnectCommandFailureEvent;
import won.bot.framework.eventbot.event.impl.command.connect.ConnectCommandResultEvent;
import won.bot.framework.eventbot.event.impl.command.feedback.FeedbackCommandEvent;
import won.bot.framework.eventbot.event.impl.command.feedback.FeedbackCommandResultEvent;
import won.bot.framework.eventbot.event.impl.command.open.OpenCommandEvent;
import won.bot.framework.eventbot.event.impl.command.open.OpenCommandResultEvent;
import won.bot.framework.eventbot.event.impl.lifecycle.ActEvent;
import won.bot.framework.eventbot.event.impl.lifecycle.InitializeEvent;
import won.bot.framework.eventbot.event.impl.lifecycle.WorkDoneEvent;
import won.bot.framework.eventbot.event.impl.needlifecycle.NeedCreatedEvent;
import won.bot.framework.eventbot.event.impl.needlifecycle.NeedProducerExhaustedEvent;
import won.bot.framework.eventbot.event.impl.wonmessage.ConnectFromOtherNeedEvent;
import won.bot.framework.eventbot.filter.impl.TargetCounterFilter;
import won.bot.framework.eventbot.listener.EventListener;
import won.bot.framework.eventbot.listener.impl.ActionOnEventListener;
import won.bot.framework.eventbot.listener.impl.ActionOnFirstEventListener;
import won.bot.framework.eventbot.listener.impl.ActionOnceAfterNEventsListener;
import won.protocol.model.Connection;
import won.protocol.vocabulary.WON;
import won.rdfimport.connectionproducer.ConnectionToCreate;
import won.rdfimport.connectionproducer.ConnectionToCreateProducer;
import won.rdfimport.event.*;

import java.net.URI;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;


/**
 * Created by fkleedorfer on 21.03.2017.
 */
public class RdfImportBot extends EventBot {

    private static final String NAME_NEEDS = "needs";
    private static final String NAME_INTERNAL_ID_TO_NEEDS = "id2needs";
    private static final String NAME_EXPECTED_INCOMING_CONNECT = "expectedConnnectFromTo";

    private ConnectionToCreateProducer connectionToCreateProducer;

    @Override
    protected void initializeEventListeners() {
        final EventListenerContext ctx = getEventListenerContext();

        final EventBus bus = getEventBus();

        final Counter needCreationSuccessfulCounter = new CounterImpl("needsCreated");
        final Counter needCreationFailedCounter = new CounterImpl("needCreationFailed");
        final Counter needCreationStartedCounter = new CounterImpl("creationStarted");

        //create a targeted counter that will publish an event when the target is reached
        //in this case, 0 unfinished need creations means that all needs were created
        final TargetCounterDecorator creationUnfinishedCounter = new TargetCounterDecorator(ctx, new CounterImpl("creationUnfinished"), 0);

        Iterator<ConnectionToCreate>[] connectionToCreateIteratorWrapper = new Iterator[1]; //will be created by one of the actions below

        //create needs every trigger execution until the need producer is exhausted
        EventListener needCreator = new ActionOnEventListener(
                ctx, "needCreator",
                new MultipleActions(ctx,
                        new IncrementCounterAction(ctx, needCreationStartedCounter),
                        new IncrementCounterAction(ctx, creationUnfinishedCounter),
                        new CreateNeedWithFacetsAction(ctx, NAME_NEEDS)
                ),
                -1
        );
        bus.subscribe(ActEvent.class, needCreator);

        //when a need is created, we have to remember the association between the need's original URI and the URI it has online
        bus.subscribe(NeedCreatedEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event) throws Exception {
                if (event instanceof NeedCreatedEvent) {
                    NeedCreatedEvent needCreatedEvent = (NeedCreatedEvent) event;
                    getBotContext().saveToObjectMap(NAME_INTERNAL_ID_TO_NEEDS,
                            needCreatedEvent.getNeedUriBeforeCreation().toString(),
                            needCreatedEvent.getNeedURI().toString());
                }
            }
        }));

        //also, keep track of what worked and what didn't
        bus.subscribe(NeedCreationFailedEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, needCreationFailedCounter)));
        bus.subscribe(NeedCreatedEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, needCreationSuccessfulCounter)));

        //when a need is created (or it failed), decrement the halfCreatedNeed counter
        EventListener downCounter = new ActionOnEventListener(ctx, "downCounter",
                new DecrementCounterAction(ctx, creationUnfinishedCounter));
        //count a successful need creation
        bus.subscribe(NeedCreatedEvent.class, downCounter);
        //if a creation failed, we don't want to keep us from keeping the correct count
        bus.subscribe(NeedCreationFailedEvent.class, downCounter);
        //we count the one execution when the creator realizes that the producer is exhausted, we have to count down
        //once for that, too.
        bus.subscribe(NeedProducerExhaustedEvent.class, downCounter);

        //just do some logging when a need is created
        bus.subscribe(NeedCreatedEvent.class, new ActionOnEventListener(ctx, "logger", new BaseEventBotAction(ctx) {
            volatile int lastOutput = 0;
            volatile long lastOutputMillis = System.currentTimeMillis();

            @Override
            protected void doRun(final Event event) throws Exception {
                int cnt = needCreationStartedCounter.getCount();
                long now = System.currentTimeMillis();
                if (cnt - lastOutput >= 100 || now - lastOutputMillis > 10000) {
                    int unfinishedCount = creationUnfinishedCounter.getCount();
                    int successCnt = needCreationSuccessfulCounter.getCount();
                    int failedCnt = needCreationFailedCounter.getCount();
                    logger.info("progress on need creation: total:{}, successful: {}, failed: {}, still waiting for response: {}",
                            new Object[]{cnt,
                                    successCnt,
                                    failedCnt,
                                    unfinishedCount});
                    lastOutput = cnt;
                    lastOutputMillis = now;
                }
            }
        }));

        //when the needproducer is exhausted, we have to wait until all unfinished need creations finish
        //when they do, the NeedGenerationFinishedEvent is published
        bus.subscribe(NeedProducerExhaustedEvent.class, new ActionOnFirstEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event) throws Exception {
                //when we're called, there probably are need creations unfinished, but there may not be
                //a)
                //first, prepare for the case when there are unfinished need creations:
                //we register a listener, waiting for the unfinished counter to reach 0
                EventListener waitForUnfinishedNeedsListener = new ActionOnFirstEventListener(ctx, new TargetCounterFilter(creationUnfinishedCounter), new PublishEventAction(ctx, new NeedGenerationFinishedEvent()));
                bus.subscribe(TargetCountReachedEvent.class, waitForUnfinishedNeedsListener);
                //now, we can check if we've already reached the target
                if (creationUnfinishedCounter.getCount() <= 0) {
                    //ok, turned out we didn't need that listener
                    bus.unsubscribe(waitForUnfinishedNeedsListener);
                    bus.publish(new NeedGenerationFinishedEvent());
                }
            }
        }));

        //When the needproducer is exhausted, stop the creator.
        getEventBus().subscribe(NeedProducerExhaustedEvent.class, new ActionOnEventListener(ctx,
                new UnsubscribeListenerAction(
                        ctx, needCreator)));



        //initialize the connection iterator
        bus.subscribe(InitializeEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event) throws Exception {
                //initialize an iterator over the connections to be created
                connectionToCreateIteratorWrapper[0] = connectionToCreateProducer.getConnectionIterator();
                bus.publish(new ConnectionProducerInitializedEvent());
            }
        }));


        //the connection creator creates one connection each time it runs, data is obtained from the connectionToCreate iterator
        EventListener connectionCreator = new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event) throws Exception {
                Iterator<ConnectionToCreate> connectionToCreateIterator = connectionToCreateIteratorWrapper[0];
                if (connectionToCreateIterator == null) return;
                if (!connectionToCreateIterator.hasNext()) {
                    bus.publish(new ConectionProducerExhaustedEvent());
                    connectionToCreateIteratorWrapper[0] = null;
                    return;
                }
                ConnectionToCreate connectionToCreate = connectionToCreateIterator.next();
                String ownNeedUriString = (String) getBotContext().loadFromObjectMap(NAME_INTERNAL_ID_TO_NEEDS, connectionToCreate.getInternalIdFrom().getURI().toString());
                String remoteNeedUriString = (String) getBotContext().loadFromObjectMap(NAME_INTERNAL_ID_TO_NEEDS, connectionToCreate.getInternalIdTo().getURI().toString());
                if (ownNeedUriString == null){
                    logger.info("cannot make connection from internal id {} because mapping to published need URI is missing", connectionToCreate.getInternalIdFrom());
                    return;
                }
                if (remoteNeedUriString == null){
                    logger.info("cannot make connection to internal id {} because mapping to published need URI is missing", connectionToCreate.getInternalIdTo());
                    return;
                }
                URI ownNeedURI = URI.create(ownNeedUriString);
                URI remoteNeedURI = URI.create(remoteNeedUriString);
                ConnectCommandEvent command = new ConnectCommandEvent(ownNeedURI, remoteNeedURI);
                bus.publish(command);
            }
        });

        //wait for two things: need creation to finish and the connection info to be available,
        //then publish the StartCreatingConnectionsCommandEvent
        EventListener connectionCreationStarter = new ActionOnceAfterNEventsListener(ctx, 2, new PublishEventAction(ctx, new StartCreatingConnectionsCommandEvent()));
        bus.subscribe(NeedGenerationFinishedEvent.class, connectionCreationStarter);
        bus.subscribe(ConnectionProducerInitializedEvent.class, connectionCreationStarter);

        //when StartCreatingConnectionsCommandEvent is received, register the connectionCreator
        bus.subscribe(StartCreatingConnectionsCommandEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event) throws Exception {
                //subscribe to the ActEvent, establishing one connection each time
                bus.subscribe(ActEvent.class, connectionCreator);
            }
        }));

        //when the ConectionProducerExhaustedEvent is exhausted, unsubscribe it
        bus.subscribe(ConectionProducerExhaustedEvent.class, new ActionOnFirstEventListener(ctx, new UnsubscribeListenerAction(ctx, connectionCreator)));

        //when the ConnectCommandEvent is published, remember which connection we're creating
        // so we can prepare to receive and accept the incoming connect message
        bus.subscribe(ConnectCommandEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event) throws Exception {
                ConnectCommandEvent connectCommandEvent = (ConnectCommandEvent) event;
                getBotContext().addToListMap(NAME_EXPECTED_INCOMING_CONNECT,
                        connectCommandEvent.getRemoteNeedURI().toString(),
                        connectCommandEvent.getNeedURI().toString());
            }
        }));

        //when the ConnectCommandEvent is published, create a connection
        bus.subscribe(ConnectCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteConnectCommandAction(ctx)));


        //if the ConnectCommand fails, we have to remote the association between the two needs because the
        //connect we're waiting for will never happen
        bus.subscribe(ConnectCommandFailureEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx){
            @Override
            protected void doRun(Event event) throws Exception {
                ConnectCommandFailureEvent connectCommandFailureEvent = (ConnectCommandFailureEvent) event;
                ConnectCommandEvent connectCommandEvent = (ConnectCommandEvent) connectCommandFailureEvent.getOriginalCommandEvent();
                getBotContext().removeFromListMap(NAME_EXPECTED_INCOMING_CONNECT,
                        connectCommandEvent.getRemoteNeedURI().toString(),
                        connectCommandEvent.getNeedURI().toString());
            }
        }));

        //if we receive a connect message, we have to check if it's an expected one
        //if so, accept and remove the association from our botContext.
        bus.subscribe(ConnectFromOtherNeedEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event) throws Exception {
                ConnectFromOtherNeedEvent connectEvent = (ConnectFromOtherNeedEvent) event;
                List<Object> expectedUriStrings = getBotContext().loadFromListMap(NAME_EXPECTED_INCOMING_CONNECT, connectEvent.getNeedURI().toString());
                if (expectedUriStrings == null) {
                    logger.debug("ignoring connect received on behalf of need {} from remote need {} because we're not expecting to be contacted by that need.", connectEvent.getNeedURI(), connectEvent.getRemoteNeedURI());
                    return;
                }
                if (! expectedUriStrings.contains(connectEvent.getRemoteNeedURI().toString())) {
                    logger.debug("ignoring connect received on behalf of need {} from remote need {} because we're not expecting to be contacted by that need", connectEvent.getNeedURI(), connectEvent.getRemoteNeedURI());
                    return;
                }
                Connection con = connectEvent.getCon();
                bus.publish(new FeedbackCommandEvent(con, con.getConnectionURI(), URI.create(WON.HAS_BINARY_RATING.getURI()), URI.create(WON.GOOD.getURI())));
                bus.publish(new OpenCommandEvent(con));
                getBotContext().removeFromListMap(NAME_EXPECTED_INCOMING_CONNECT,
                        connectEvent.getRemoteNeedURI().toString(),
                        connectEvent.getNeedURI().toString());
                }
            }
        ));

        //open the connection if the OpenCommandEvent is published
        bus.subscribe(OpenCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteOpenCommandAction(ctx)));
        //send feedback when the FeedbackCommandEvent is published
        bus.subscribe(FeedbackCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteFeedbackCommandAction(ctx)));


        //we count connect commands, and we know we're finished as soon as we've seen the results
        // to as many CONNECT, OPEN, and HINT_FEEDBACK messages as we've seen connect commands
        final Counter connectCommandCounter = new CounterImpl("connectCommandCounter");
        bus.subscribe(ConnectCommandEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx,connectCommandCounter)));

        final Counter connectCommandResultCounter = new CounterImpl("connectCommandResultCounter");
        bus.subscribe(ConnectCommandResultEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, connectCommandResultCounter)));
        final Counter openCommandResultCounter = new CounterImpl("openCommandResultCounter");
        bus.subscribe(OpenCommandResultEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, openCommandResultCounter)));
        final Counter feedbackCommandResultCounter = new CounterImpl("feedbackCommandResultCounter");
        bus.subscribe(FeedbackCommandResultEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, feedbackCommandResultCounter)));


        //each time one of these counters counts, we check if we're done
        //this listener is registered when creating connections is done
        BotTrigger checkFinishedTrigger = new BotTrigger(ctx, Duration.ofSeconds(10));
        checkFinishedTrigger.activate();
        //when all the responses have been received, start the trigger that is used for checking if finished
        bus.subscribe(NeedGenerationFinishedEvent.class, new ActionOnEventListener(ctx, new PublishEventAction(ctx, new StartBotTriggerCommandEvent(checkFinishedTrigger))));

        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, "checkIfFinished", checkFinishedTrigger, new BaseEventBotAction(ctx){
            @Override
            protected void doRun(Event event) throws Exception {
                int target = connectCommandCounter.getCount();
                if (target == 0) {
                    //seems we haven't started connecting yet.
                    return;
                }
                if (feedbackCommandResultCounter.getCount() < target) return;
                if (openCommandResultCounter.getCount() < target) return;
                if (connectCommandResultCounter.getCount() < target) return;
                bus.publish(new StopBotTriggerCommandEvent(checkFinishedTrigger));
                bus.publish(new WorkDoneEvent(RdfImportBot.this));
                logger.info("finishing...");
            }
        }));

        //just do some logging when a command result is received
        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, "logger2", checkFinishedTrigger, new BaseEventBotAction(ctx) {
            volatile long lastOutputMillis = System.currentTimeMillis();

            @Override
            protected void doRun(final Event event) throws Exception {
                    logger.info("progress on connection creation: connects sent: {} connects finished: {}, opens finished: {}, feedbacks finished: {}",
                            new Object[]{ connectCommandCounter.getCount(),
                                    connectCommandResultCounter.getCount(),
                                    openCommandResultCounter.getCount(),
                                    feedbackCommandResultCounter.getCount()});
            }
        }));

    }


    public void setConnectionToCreateProducer(ConnectionToCreateProducer connectionToCreateProducer) {
        this.connectionToCreateProducer = connectionToCreateProducer;
    }
}
