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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import won.protocol.vocabulary.WONCON;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Resource;

import won.bot.framework.bot.base.EventBot;
import won.bot.framework.component.atomproducer.AtomProducer;
import won.bot.framework.eventbot.EventListenerContext;
import won.bot.framework.eventbot.action.BaseEventBotAction;
import won.bot.framework.eventbot.action.impl.MultipleActions;
import won.bot.framework.eventbot.action.impl.PublishEventAction;
import won.bot.framework.eventbot.action.impl.counter.Counter;
import won.bot.framework.eventbot.action.impl.counter.CounterImpl;
import won.bot.framework.eventbot.action.impl.counter.DecrementCounterAction;
import won.bot.framework.eventbot.action.impl.counter.IncrementCounterAction;
import won.bot.framework.eventbot.action.impl.counter.TargetCountReachedEvent;
import won.bot.framework.eventbot.action.impl.counter.TargetCounterDecorator;
import won.bot.framework.eventbot.action.impl.maintenance.StatisticsLoggingAction;
import won.bot.framework.eventbot.action.impl.trigger.ActionOnTriggerEventListener;
import won.bot.framework.eventbot.action.impl.trigger.AddFiringsAction;
import won.bot.framework.eventbot.action.impl.trigger.BotTrigger;
import won.bot.framework.eventbot.action.impl.trigger.BotTriggerEvent;
import won.bot.framework.eventbot.action.impl.trigger.FireCountLimitedBotTrigger;
import won.bot.framework.eventbot.action.impl.trigger.StartBotTriggerCommandEvent;
import won.bot.framework.eventbot.action.impl.trigger.StopBotTriggerCommandEvent;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.ExecuteConnectCommandAction;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.ExecuteCreateAtomCommandAction;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.ExecuteFeedbackCommandAction;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.ExecuteOpenCommandAction;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.LogMessageCommandFailureAction;
import won.bot.framework.eventbot.bus.EventBus;
import won.bot.framework.eventbot.event.Event;
import won.bot.framework.eventbot.event.impl.command.MessageCommandEvent;
import won.bot.framework.eventbot.event.impl.command.MessageCommandFailureEvent;
import won.bot.framework.eventbot.event.impl.command.MessageCommandResultEvent;
import won.bot.framework.eventbot.event.impl.command.connect.ConnectCommandEvent;
import won.bot.framework.eventbot.event.impl.command.connect.ConnectCommandFailureEvent;
import won.bot.framework.eventbot.event.impl.command.connect.ConnectCommandResultEvent;
import won.bot.framework.eventbot.event.impl.command.connect.ConnectCommandSuccessEvent;
import won.bot.framework.eventbot.event.impl.command.create.CreateAtomCommandEvent;
import won.bot.framework.eventbot.event.impl.command.create.CreateAtomCommandFailureEvent;
import won.bot.framework.eventbot.event.impl.command.create.CreateAtomCommandResultEvent;
import won.bot.framework.eventbot.event.impl.command.create.CreateAtomCommandSuccessEvent;
import won.bot.framework.eventbot.event.impl.command.feedback.FeedbackCommandEvent;
import won.bot.framework.eventbot.event.impl.command.feedback.FeedbackCommandSuccessEvent;
import won.bot.framework.eventbot.event.impl.command.open.OpenCommandEvent;
import won.bot.framework.eventbot.event.impl.command.open.OpenCommandSuccessEvent;
import won.bot.framework.eventbot.event.impl.lifecycle.InitializeEvent;
import won.bot.framework.eventbot.event.impl.lifecycle.WorkDoneEvent;
import won.bot.framework.eventbot.event.impl.atomlifecycle.AtomProducerExhaustedEvent;
import won.bot.framework.eventbot.event.impl.wonmessage.ConnectFromOtherAtomEvent;
import won.bot.framework.eventbot.event.impl.wonmessage.OpenFromOtherAtomEvent;
import won.bot.framework.eventbot.filter.impl.TargetCounterFilter;
import won.bot.framework.eventbot.listener.EventListener;
import won.bot.framework.eventbot.listener.impl.ActionOnEventListener;
import won.bot.framework.eventbot.listener.impl.ActionOnFirstEventListener;
import won.bot.framework.eventbot.listener.impl.ActionOnceAfterNEventsListener;
import won.protocol.model.Connection;
import won.protocol.util.WonRdfUtils;
import won.protocol.vocabulary.CNT;
import won.protocol.vocabulary.WON;
import won.rdfimport.connectionproducer.ConnectionToCreate;
import won.rdfimport.connectionproducer.ConnectionToCreateProducer;
import won.rdfimport.event.ConectionProducerExhaustedEvent;
import won.rdfimport.event.ConnectionCreationSkippedEvent;
import won.rdfimport.event.ConnectionProducerInitializedEvent;
import won.rdfimport.event.AtomCreationSkippedEvent;
import won.rdfimport.event.AtomGenerationFinishedEvent;
import won.rdfimport.event.StartCreatingConnectionsCommandEvent;
import won.rdfimport.event.StartCreatingAtomsCommandEvent;


/**
 * Created by fkleedorfer on 21.03.2017.
 */
public class RdfImportBot extends EventBot {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String NAME_INTERNAL_ID_TO_ATOMS = "id2atoms";
    private static final String NAME_EXPECTED_INCOMING_CONNECT = "expectedConnnectFromTo";
    private static final String NAME_PROCESSED_CONNECTIONS = "processedConnectionsFromTo";

    private ConnectionToCreateProducer connectionToCreateProducer;

    private int targetInflightCount = 5;
    private int maxInflightCount = 10;

    public void setTargetInflightCount(int targetInflightCount) {
        this.targetInflightCount = targetInflightCount;
    }

    public void setMaxInflightCount(int maxInflightCount) {
        this.maxInflightCount = maxInflightCount;
    }

    @Override
    protected void initializeEventListeners() {
        boolean skipAtomCreation = false;
        final EventListenerContext ctx = getEventListenerContext();

        final EventBus bus = getEventBus();
        Counter messagesInflightCounter = new CounterImpl("messagesInflightCounter");
        bus.subscribe(CreateAtomCommandEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, messagesInflightCounter)));
        bus.subscribe(CreateAtomCommandResultEvent.class, new ActionOnEventListener(ctx, new DecrementCounterAction(ctx, messagesInflightCounter)));
        bus.subscribe(ConnectCommandEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, messagesInflightCounter)));
        bus.subscribe(ConnectCommandResultEvent.class, new ActionOnEventListener(ctx, new DecrementCounterAction(ctx, messagesInflightCounter)));
        Counter messageCommandCounter = new CounterImpl("MessageCommandCounter");
        bus.subscribe(MessageCommandEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, messageCommandCounter)));
        final Counter atomCreationSuccessfulCounter = new CounterImpl("atomsCreated");
        final Counter atomCreationSkippedCounter = new CounterImpl("atomCreationSkipped");
        final Counter atomCreationFailedCounter = new CounterImpl("atomCreationFailed");
        final Counter atomCreationStartedCounter = new CounterImpl("creationStarted");
        
        //create a targeted counter that will publish an event when the target is reached
        //in this case, 0 unfinished atom creations means that all atoms were created
        final TargetCounterDecorator creationUnfinishedCounter = new TargetCounterDecorator(ctx, new CounterImpl("creationUnfinished"), 0);

        Iterator<ConnectionToCreate>[] connectionToCreateIteratorWrapper = new Iterator[1]; //will be created by one of the actions below

        //if we receive a message command failure, log it
        bus.subscribe(MessageCommandFailureEvent.class, new ActionOnEventListener(ctx, new LogMessageCommandFailureAction(ctx)));
        //if we receive a message command, execute it
        bus.subscribe(CreateAtomCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteCreateAtomCommandAction(ctx)));
        bus.subscribe(ConnectCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteConnectCommandAction(ctx)));
        bus.subscribe(OpenCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteOpenCommandAction(ctx)));
        bus.subscribe(FeedbackCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteFeedbackCommandAction(ctx)));

        //use a trigger that can only fire N times before being allowed more firings (controlling the speed)
        FireCountLimitedBotTrigger createAtomTrigger = new FireCountLimitedBotTrigger(ctx, Duration.ofMillis(100), maxInflightCount);
        // each time we hear back from a connect, allow the trigger to fire once more
        bus.subscribe(CreateAtomCommandResultEvent.class, new ActionOnEventListener(ctx, new AddFiringsAction(ctx, createAtomTrigger, 1)));
        createAtomTrigger.activate();

        bus.subscribe(StartCreatingAtomsCommandEvent.class, new ActionOnFirstEventListener(ctx, new PublishEventAction(ctx, new StartBotTriggerCommandEvent(createAtomTrigger))));

        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, createAtomTrigger, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                AtomProducer atomProducer = getEventListenerContext().getAtomProducer();
                Dataset model = atomProducer.create();
                if (model == null && atomProducer.isExhausted()) {
                    bus.publish(new AtomProducerExhaustedEvent());
                    bus.unsubscribe(executingListener);
                    return;
                }
                URI atomUriFromProducer = null;
                Resource atomResource = WonRdfUtils.AtomUtils.getAtomResource(model);
                if (atomResource.isURIResource()) {
                    atomUriFromProducer = URI.create(atomResource.getURI().toString());
                }
                if (atomUriFromProducer != null) {
                    String atomURI = (String) getBotContextWrapper().getBotContext().loadFromObjectMap(NAME_INTERNAL_ID_TO_ATOMS,
                            atomUriFromProducer.toString());
                    if (atomURI != null) {
                        bus.publish(new AtomCreationSkippedEvent());
                    } else {
                        bus.publish(new CreateAtomCommandEvent(model, getBotContextWrapper().getAtomCreateListName(), false, false));
                    }
                }
            }
        }));

        bus.subscribe(CreateAtomCommandEvent.class, new ActionOnEventListener(ctx, new MultipleActions(ctx,
                new IncrementCounterAction(ctx, atomCreationStartedCounter),
                new IncrementCounterAction(ctx, creationUnfinishedCounter))));



        //when an atom is created, we have to remember the association between the atom's original URI and the URI it has online
        bus.subscribe(CreateAtomCommandSuccessEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                if (event instanceof CreateAtomCommandSuccessEvent) {
                    CreateAtomCommandSuccessEvent atomCreatedEvent = (CreateAtomCommandSuccessEvent) event;
                    getBotContextWrapper().getBotContext().saveToObjectMap(NAME_INTERNAL_ID_TO_ATOMS,
                            atomCreatedEvent.getAtomUriBeforeCreation().toString(),
                            atomCreatedEvent.getAtomURI().toString());
                }
            }
        }));

        //also, keep track of what worked and what didn't
        bus.subscribe(CreateAtomCommandFailureEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, atomCreationFailedCounter)));
        bus.subscribe(CreateAtomCommandSuccessEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, atomCreationSuccessfulCounter)));
        bus.subscribe(AtomCreationSkippedEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, atomCreationSkippedCounter)));

        //when an atom is created (or it failed), decrement the halfCreatedAtom counter
        EventListener downCounter = new ActionOnEventListener(ctx, "downCounter",
                new DecrementCounterAction(ctx, creationUnfinishedCounter));
        //count a successful atom creation
        bus.subscribe(CreateAtomCommandSuccessEvent.class, downCounter);
        //if a creation failed, we don't want to keep us from keeping the correct count
        bus.subscribe(CreateAtomCommandFailureEvent.class, downCounter);

        //trigger statistics logging about atom generation
        BotTrigger atomCreationStatsLoggingTrigger = new BotTrigger(ctx, Duration.ofSeconds(10));
        
        atomCreationStatsLoggingTrigger.activate();
        bus.subscribe(StartCreatingAtomsCommandEvent.class, new ActionOnFirstEventListener(ctx, new PublishEventAction(ctx, new StartBotTriggerCommandEvent(atomCreationStatsLoggingTrigger))));
        //just do some logging when an atom is created
        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, "creationStatsLogger", atomCreationStatsLoggingTrigger, new BaseEventBotAction(ctx) {
            volatile int lastOutput = 0;
            volatile long lastOutputMillis = System.currentTimeMillis();

            @Override
            protected void doRun(final Event event, EventListener executingListener) throws Exception {
                int cnt = atomCreationStartedCounter.getCount();
                long now = System.currentTimeMillis();
                long millisSinceLastOutput = now-lastOutputMillis;
                int countSinceLastOutput = cnt-lastOutput;
                double countPerSecond = (double) countSinceLastOutput / ((double) millisSinceLastOutput / 1000d);
                logger.info("progress on atom creation: total:{}, successful: {}, failed: {}, still waiting for response: {}, skipped: {}, messages inflight: {} (max: {}), create trigger delay: {} millis, millis since last output: {}, started since last ouput: {} ({} per second)",
                        new Object[]{cnt,
                                atomCreationSuccessfulCounter.getCount(),
                                atomCreationFailedCounter.getCount(),
                                creationUnfinishedCounter.getCount(),
                                atomCreationSkippedCounter.getCount(),
                                messagesInflightCounter.getCount(),
                                maxInflightCount,
                                createAtomTrigger.getInterval().toMillis(),
                                millisSinceLastOutput,
                                countSinceLastOutput,
                                String.format("%.2f", countPerSecond)});
                lastOutput = cnt;
                lastOutputMillis = now;
            }
        }));

        //stop the stats logging trigger when atom generation is finished
        bus.subscribe(AtomGenerationFinishedEvent.class, new ActionOnFirstEventListener(ctx, new PublishEventAction(ctx, new StopBotTriggerCommandEvent(atomCreationStatsLoggingTrigger))));

        //when the atomproducer is exhausted, we have to wait until all unfinished atom creations finish
        //when they do, the AtomGenerationFinishedEvent is published
        bus.subscribe(AtomProducerExhaustedEvent.class, new ActionOnFirstEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                //when we're called, there probably are atom creations unfinished, but there may not be
                //a)
                //first, prepare for the case when there are unfinished atom creations:
                //we register a listener, waiting for the unfinished counter to reach 0
                EventListener waitForUnfinishedAtomsListener = new ActionOnFirstEventListener(ctx, new TargetCounterFilter(creationUnfinishedCounter), new PublishEventAction(ctx, new AtomGenerationFinishedEvent()));
                bus.subscribe(TargetCountReachedEvent.class, waitForUnfinishedAtomsListener);
                //now, we can check if we've already reached the target
                if (creationUnfinishedCounter.getCount() <= 0) {
                    //ok, turned out we didn't atom that listener
                    bus.unsubscribe(waitForUnfinishedAtomsListener);
                    bus.publish(new AtomGenerationFinishedEvent());
                }
            }
        }));

        //When the atomproducer is exhausted, stop the creator.
        getEventBus().subscribe(AtomProducerExhaustedEvent.class, new ActionOnFirstEventListener(ctx,
                new PublishEventAction(ctx, new StopBotTriggerCommandEvent(createAtomTrigger))));

        //initialize the connection iterator
        bus.subscribe(InitializeEvent.class, new ActionOnFirstEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                //initialize an iterator over the connections to be created
                connectionToCreateIteratorWrapper[0] = connectionToCreateProducer.getConnectionIterator();
                bus.publish(new ConnectionProducerInitializedEvent());
            }
        }));


        //the connection creator creates one connection each time it runs, data is obtained from the connectionToCreate iterator

        //wait for two things: atom creation to finish and the connection info to be available,
        //then publish the StartCreatingConnectionsCommandEvent
        EventListener connectionCreationStarter = new ActionOnceAfterNEventsListener(ctx, 2, new PublishEventAction(ctx, new StartCreatingConnectionsCommandEvent()));
        bus.subscribe(AtomGenerationFinishedEvent.class, connectionCreationStarter);
        bus.subscribe(ConnectionProducerInitializedEvent.class, connectionCreationStarter);

        //use a trigger that can only fire N times before being allowed more firings (controlling the speed)
        FireCountLimitedBotTrigger createConnectionsTrigger = new FireCountLimitedBotTrigger(ctx, Duration.ofMillis(100), maxInflightCount);
        // each time we hear back from a connect, allow the trigger to fire once more
        bus.subscribe(ConnectCommandResultEvent.class, new ActionOnEventListener(ctx, new AddFiringsAction(ctx, createAtomTrigger, 1)));

        //start the connections trigger when we are done creating atoms.
        bus.subscribe(StartCreatingConnectionsCommandEvent.class, new ActionOnFirstEventListener(ctx, new BaseEventBotAction(ctx) {
          @Override
          protected void doRun(Event event, EventListener executingListener) throws Exception {
            createConnectionsTrigger.activate();
          }
        }));
        

        bus.subscribe(StartCreatingConnectionsCommandEvent.class, new ActionOnFirstEventListener(ctx, new PublishEventAction(ctx, new StartBotTriggerCommandEvent(createConnectionsTrigger))));
        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, createConnectionsTrigger, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                //create connection
                Iterator<ConnectionToCreate> connectionToCreateIterator = connectionToCreateIteratorWrapper[0];
                if (connectionToCreateIterator == null) return;

                ConnectionToCreate connectionToCreate = null;
                String ownAtomUriString = null;
                String targetAtomUriString = null;
                while (connectionToCreate == null) {
                    connectionToCreate = connectionToCreateIterator.next();
                    if (connectionToCreate == null) {
                        bus.publish(new ConectionProducerExhaustedEvent());
                        bus.unsubscribe(executingListener);
                        connectionToCreateIteratorWrapper[0] = null;
                        return;
                    }

                    ownAtomUriString = (String) getBotContextWrapper().getBotContext().loadFromObjectMap(NAME_INTERNAL_ID_TO_ATOMS, connectionToCreate.getInternalIdFrom().getURI().toString());
                    targetAtomUriString = (String) getBotContextWrapper().getBotContext().loadFromObjectMap(NAME_INTERNAL_ID_TO_ATOMS, connectionToCreate.getInternalIdTo().getURI().toString());
                    List<Object> processedConnections = getBotContextWrapper().getBotContext().loadFromListMap(NAME_PROCESSED_CONNECTIONS, ownAtomUriString);
                    if (processedConnections.contains(targetAtomUriString)){
                        //we've already processed this connection in an earlier run of the bot
                        bus.publish(new ConnectionCreationSkippedEvent());
                        return;
                    }
                    if (ownAtomUriString == null) {
                        logger.debug("cannot make connection from internal id {} because mapping to published atom URI is missing", connectionToCreate.getInternalIdFrom());
                        connectionToCreate = null;
                    } else if (targetAtomUriString == null) {
                        logger.debug("cannot make connection to internal id {} because mapping to published atom URI is missing", connectionToCreate.getInternalIdTo());
                        connectionToCreate = null;
                    }
                }

                if (connectionToCreate != null) {
                    URI ownAtomURI = URI.create(ownAtomUriString);
                    URI targetAtomURI = URI.create(targetAtomUriString);
                    ConnectCommandEvent command = new ConnectCommandEvent(ownAtomURI, targetAtomURI);
                    bus.publish(command);
                }
            }
        }));

        //when the ConnectCommandEvent is published, remember which connection we're creating
        // so we can prepare to receive and accept the incoming connect message
        bus.subscribe(ConnectCommandEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                ConnectCommandEvent connectCommandEvent = (ConnectCommandEvent) event;
                getBotContextWrapper().getBotContext().addToListMap(NAME_EXPECTED_INCOMING_CONNECT,
                        connectCommandEvent.getTargetAtomURI().toString(),
                        connectCommandEvent.getAtomURI().toString());
            }
        }));



        //if the ConnectCommand fails, we have to remove the association between the two atoms because the
        //connect we're waiting for will never happen
        bus.subscribe(ConnectCommandFailureEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx){
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                ConnectCommandFailureEvent connectCommandFailureEvent = (ConnectCommandFailureEvent) event;
                ConnectCommandEvent connectCommandEvent = (ConnectCommandEvent) connectCommandFailureEvent.getOriginalCommandEvent();
                getBotContextWrapper().getBotContext().removeFromListMap(NAME_EXPECTED_INCOMING_CONNECT,
                        connectCommandEvent.getTargetAtomURI().toString(),
                        connectCommandEvent.getAtomURI().toString());
            }
        }));



        //if we receive a connect message, we have to check if it's an expected one
        //if so, accept and remove the association from our botContext.
        bus.subscribe(ConnectFromOtherAtomEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                ConnectFromOtherAtomEvent connectEvent = (ConnectFromOtherAtomEvent) event;
                List<Object> expectedUriStrings = getBotContextWrapper().getBotContext().loadFromListMap(NAME_EXPECTED_INCOMING_CONNECT, connectEvent.getAtomURI().toString());
                if (expectedUriStrings == null) {
                    logger.debug("ignoring connect received on behalf of atom {} from remote atom {} because we're not expecting to be contacted by that atom.", connectEvent.getAtomURI(), connectEvent.getTargetAtomURI());
                    return;
                }
                if (! expectedUriStrings.contains(connectEvent.getTargetAtomURI().toString())) {
                    logger.debug("ignoring connect received on behalf of atom {} from remote atom {} because we're not expecting to be contacted by that atom", connectEvent.getAtomURI(), connectEvent.getTargetAtomURI());
                    return;
                }
                Connection con = connectEvent.getCon();
                bus.publish(new OpenCommandEvent(con));
                getBotContextWrapper().getBotContext().removeFromListMap(NAME_EXPECTED_INCOMING_CONNECT,
                        connectEvent.getTargetAtomURI().toString(),
                        connectEvent.getAtomURI().toString());
                }
            }
        ));

        //we send the feedback after we've received the open command from the atom we connected to
        bus.subscribe(OpenFromOtherAtomEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                Connection con = ((OpenFromOtherAtomEvent)event).getCon();
                bus.publish(new FeedbackCommandEvent(con, con.getConnectionURI(), URI.create(WONCON.binaryRating.getURI()), URI.create(WONCON.Good.getURI())));
            }
        }));

        //if the feedback command succeeds, the workflow is finished for that connection. We have to remember that we
        // made the association, so we don't try it the next time the bot runs
        bus.subscribe(FeedbackCommandSuccessEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                FeedbackCommandSuccessEvent feedbackCommandSuccessEvent = (FeedbackCommandSuccessEvent) event;
                getBotContextWrapper().getBotContext().addToListMap(NAME_PROCESSED_CONNECTIONS,
                        feedbackCommandSuccessEvent.getAtomURI().toString(),
                        feedbackCommandSuccessEvent.getTargetAtomURI().toString());
            }
        }));

        //we count connect commands, and we know we're finished as soon as we've seen the results
        // to as many CONNECT, OPEN, and HINT_FEEDBACK messages as we've seen connect commands
        final Counter connectSkippedCounter= new CounterImpl("connectSkippedCounter");
        final Counter connectCommandCounter = new CounterImpl("connectCommandCounter");
        final Counter connectCommandResultCounter = new CounterImpl("connectCommandResultCounter");
        final Counter openCommandResultCounter = new CounterImpl("openCommandResultCounter");
        final Counter feedbackCommandResultCounter = new CounterImpl("feedbackCommandResultCounter");
        bus.subscribe(ConnectionCreationSkippedEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx,connectSkippedCounter)));
        bus.subscribe(ConnectCommandEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx,connectCommandCounter)));
        bus.subscribe(ConnectCommandSuccessEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, connectCommandResultCounter)));
        bus.subscribe(OpenCommandSuccessEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, openCommandResultCounter)));
        bus.subscribe(FeedbackCommandSuccessEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, feedbackCommandResultCounter)));


        //each time one of these counters counts, we check if we're done
        //this listener is registered when creating connections is done
        BotTrigger checkFinishedTrigger = new BotTrigger(ctx, Duration.ofSeconds(10));
        checkFinishedTrigger.activate();
        //when we start connecting, also start the trigger that is used for checking if finished
        bus.subscribe(StartCreatingConnectionsCommandEvent.class, new ActionOnEventListener(ctx, new PublishEventAction(ctx, new StartBotTriggerCommandEvent(checkFinishedTrigger))));

        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, "checkIfFinished", checkFinishedTrigger, new BaseEventBotAction(ctx){
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                if (connectionToCreateIteratorWrapper[0] != null && connectionToCreateIteratorWrapper[0].hasNext()) return;
                int target = connectCommandCounter.getCount();
                if (target == 0 && connectSkippedCounter.getCount() == 0) {
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
            volatile int lastOutput = 0;
            volatile long lastOutputMillis = System.currentTimeMillis();

            @Override
            protected void doRun(final Event event, EventListener executingListener) throws Exception {

                int cnt = connectCommandCounter.getCount();
                long now = System.currentTimeMillis();
                long millisSinceLastOutput = now-lastOutputMillis;
                int countSinceLastOutput = cnt-lastOutput;
                double countPerSecond = (double) countSinceLastOutput / ((double) millisSinceLastOutput / 1000d);
                logger.info("progress on connection creation: connects sent: {} finished: {}, opens finished: {}, feedbacks finished: {}, skipped: {}, inflight {} (max: {}), connect trigger delay: {} millis, messages sent: {}, millis since last output: {}, started since last ouput: {} ({} per second)",
                            new Object[]{ connectCommandCounter.getCount(),
                                    connectCommandResultCounter.getCount(),
                                    openCommandResultCounter.getCount(),
                                    feedbackCommandResultCounter.getCount(),
                                    connectSkippedCounter.getCount(),
                                    messagesInflightCounter.getCount(),
                                    maxInflightCount,
                                    createConnectionsTrigger.getInterval().toMillis(),
                                    messageCommandCounter.getCount(),
                                    millisSinceLastOutput,
                                    countSinceLastOutput,
                                    String.format("%.2f", countPerSecond)
                            });
                lastOutput = cnt;
                lastOutputMillis = now;
            }
        }));

        //show the EventBotStatistics from time to time
        BotTrigger statisticsLoggingTrigger = new BotTrigger(ctx, Duration.ofSeconds(30));
        statisticsLoggingTrigger.activate();
        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, "statslogger", statisticsLoggingTrigger, new StatisticsLoggingAction(ctx)));
        bus.publish(new StartBotTriggerCommandEvent(statisticsLoggingTrigger));

        if (skipAtomCreation) {
            bus.publish(new StartCreatingConnectionsCommandEvent());
        } else {
            bus.publish(new StartCreatingAtomsCommandEvent());
        }
    }

    private boolean isTooManyMessagesInflight(Counter messagesInflightCounter) {
        //cut off if too many messages are already in-flight
        if (messagesInflightCounter.getCount() > maxInflightCount) {
            return true;
        }
        return false;
    }
   

    public void setConnectionToCreateProducer(ConnectionToCreateProducer connectionToCreateProducer) {
        this.connectionToCreateProducer = connectionToCreateProducer;
    }
}
