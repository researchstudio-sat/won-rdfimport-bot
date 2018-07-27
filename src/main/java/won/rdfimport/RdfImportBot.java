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

import java.net.URI;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Resource;

import won.bot.framework.bot.base.EventBot;
import won.bot.framework.component.needproducer.NeedProducer;
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
import won.bot.framework.eventbot.action.impl.trigger.BotTrigger;
import won.bot.framework.eventbot.action.impl.trigger.BotTriggerEvent;
import won.bot.framework.eventbot.action.impl.trigger.StartBotTriggerCommandEvent;
import won.bot.framework.eventbot.action.impl.trigger.StopBotTriggerCommandEvent;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.ExecuteConnectCommandAction;
import won.bot.framework.eventbot.action.impl.wonmessage.execCommand.ExecuteCreateNeedCommandAction;
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
import won.bot.framework.eventbot.event.impl.command.connect.ConnectCommandSuccessEvent;
import won.bot.framework.eventbot.event.impl.command.create.CreateNeedCommandEvent;
import won.bot.framework.eventbot.event.impl.command.create.CreateNeedCommandFailureEvent;
import won.bot.framework.eventbot.event.impl.command.create.CreateNeedCommandSuccessEvent;
import won.bot.framework.eventbot.event.impl.command.feedback.FeedbackCommandEvent;
import won.bot.framework.eventbot.event.impl.command.feedback.FeedbackCommandSuccessEvent;
import won.bot.framework.eventbot.event.impl.command.open.OpenCommandEvent;
import won.bot.framework.eventbot.event.impl.command.open.OpenCommandSuccessEvent;
import won.bot.framework.eventbot.event.impl.lifecycle.InitializeEvent;
import won.bot.framework.eventbot.event.impl.lifecycle.WorkDoneEvent;
import won.bot.framework.eventbot.event.impl.needlifecycle.NeedProducerExhaustedEvent;
import won.bot.framework.eventbot.event.impl.wonmessage.ConnectFromOtherNeedEvent;
import won.bot.framework.eventbot.event.impl.wonmessage.OpenFromOtherNeedEvent;
import won.bot.framework.eventbot.filter.impl.TargetCounterFilter;
import won.bot.framework.eventbot.listener.EventListener;
import won.bot.framework.eventbot.listener.impl.ActionOnEventListener;
import won.bot.framework.eventbot.listener.impl.ActionOnFirstEventListener;
import won.bot.framework.eventbot.listener.impl.ActionOnceAfterNEventsListener;
import won.protocol.model.Connection;
import won.protocol.util.WonRdfUtils;
import won.protocol.vocabulary.WON;
import won.rdfimport.connectionproducer.ConnectionToCreate;
import won.rdfimport.connectionproducer.ConnectionToCreateProducer;
import won.rdfimport.event.ConectionProducerExhaustedEvent;
import won.rdfimport.event.ConnectionCreationSkippedEvent;
import won.rdfimport.event.ConnectionProducerInitializedEvent;
import won.rdfimport.event.NeedCreationSkippedEvent;
import won.rdfimport.event.NeedGenerationFinishedEvent;
import won.rdfimport.event.StartCreatingConnectionsCommandEvent;
import won.rdfimport.event.StartCreatingNeedsCommandEvent;


/**
 * Created by fkleedorfer on 21.03.2017.
 */
public class RdfImportBot extends EventBot {
    private static final String NAME_INTERNAL_ID_TO_NEEDS = "id2needs";
    private static final String NAME_EXPECTED_INCOMING_CONNECT = "expectedConnnectFromTo";
    private static final String NAME_PROCESSED_CONNECTIONS = "processedConnectionsFromTo";

    private ConnectionToCreateProducer connectionToCreateProducer;

    private int targetInflightCount = 30;
    private int maxInflightCount = 50;

    public void setTargetInflightCount(int targetInflightCount) {
        this.targetInflightCount = targetInflightCount;
    }

    public void setMaxInflightCount(int maxInflightCount) {
        this.maxInflightCount = maxInflightCount;
    }

    @Override
    protected void initializeEventListeners() {
        boolean skipNeedCreation = false;
        final EventListenerContext ctx = getEventListenerContext();

        final EventBus bus = getEventBus();
        Counter messagesInflightCounter = new CounterImpl("messagesInflightCounter");
        bus.subscribe(MessageCommandEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, messagesInflightCounter)));
        bus.subscribe(MessageCommandResultEvent.class, new ActionOnEventListener(ctx, new DecrementCounterAction(ctx, messagesInflightCounter)));
        Counter messageCommandCounter = new CounterImpl("MessageCommandCounter");
        bus.subscribe(MessageCommandEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, messageCommandCounter)));
        final Counter needCreationSuccessfulCounter = new CounterImpl("needsCreated");
        final Counter needCreationSkippedCounter = new CounterImpl("needCreationSkipped");
        final Counter needCreationFailedCounter = new CounterImpl("needCreationFailed");
        final Counter needCreationStartedCounter = new CounterImpl("creationStarted");

        //create a targeted counter that will publish an event when the target is reached
        //in this case, 0 unfinished need creations means that all needs were created
        final TargetCounterDecorator creationUnfinishedCounter = new TargetCounterDecorator(ctx, new CounterImpl("creationUnfinished"), 0);

        Iterator<ConnectionToCreate>[] connectionToCreateIteratorWrapper = new Iterator[1]; //will be created by one of the actions below

        //if we receive a message command failure, log it
        bus.subscribe(MessageCommandFailureEvent.class, new ActionOnEventListener(ctx, new LogMessageCommandFailureAction(ctx)));
        //if we receive a message command, execute it
        bus.subscribe(CreateNeedCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteCreateNeedCommandAction(ctx)));
        bus.subscribe(ConnectCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteConnectCommandAction(ctx)));
        bus.subscribe(OpenCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteOpenCommandAction(ctx)));
        bus.subscribe(FeedbackCommandEvent.class, new ActionOnEventListener(ctx, new ExecuteFeedbackCommandAction(ctx)));


        BotTrigger createNeedTrigger = new BotTrigger(ctx, Duration.ofMillis(100));
        createNeedTrigger.activate();

        bus.subscribe(StartCreatingNeedsCommandEvent.class, new ActionOnFirstEventListener(ctx, new PublishEventAction(ctx, new StartBotTriggerCommandEvent(createNeedTrigger))));

        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, createNeedTrigger, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                if (isTooManyMessagesInflight(messagesInflightCounter)) {
                    return;
                }
                adjustTriggerInterval(createNeedTrigger, messagesInflightCounter);
                NeedProducer needProducer = getEventListenerContext().getNeedProducer();
                Dataset model = needProducer.create();
                if (model == null && needProducer.isExhausted()) {
                    bus.publish(new NeedProducerExhaustedEvent());
                    bus.unsubscribe(executingListener);
                    return;
                }
                URI needUriFromProducer = null;
                Resource needResource = WonRdfUtils.NeedUtils.getNeedResource(model);
                if (needResource.isURIResource()) {
                    needUriFromProducer = URI.create(needResource.getURI().toString());
                }
                if (needUriFromProducer != null) {
                    String needURI = (String) getBotContextWrapper().getBotContext().loadFromObjectMap(NAME_INTERNAL_ID_TO_NEEDS,
                            needUriFromProducer.toString());
                    if (needURI != null) {
                        bus.publish(new NeedCreationSkippedEvent());
                    } else {
                        bus.publish(new CreateNeedCommandEvent(model, getBotContextWrapper().getNeedCreateListName(), true, false));
                    }
                }
            }
        }));

        bus.subscribe(CreateNeedCommandEvent.class, new ActionOnEventListener(ctx, new MultipleActions(ctx,
                new IncrementCounterAction(ctx, needCreationStartedCounter),
                new IncrementCounterAction(ctx, creationUnfinishedCounter))));



        //when a need is created, we have to remember the association between the need's original URI and the URI it has online
        bus.subscribe(CreateNeedCommandSuccessEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                if (event instanceof CreateNeedCommandSuccessEvent) {
                    CreateNeedCommandSuccessEvent needCreatedEvent = (CreateNeedCommandSuccessEvent) event;
                    getBotContextWrapper().getBotContext().saveToObjectMap(NAME_INTERNAL_ID_TO_NEEDS,
                            needCreatedEvent.getNeedUriBeforeCreation().toString(),
                            needCreatedEvent.getNeedURI().toString());
                }
            }
        }));

        //also, keep track of what worked and what didn't
        bus.subscribe(CreateNeedCommandFailureEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, needCreationFailedCounter)));
        bus.subscribe(CreateNeedCommandSuccessEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, needCreationSuccessfulCounter)));
        bus.subscribe(NeedCreationSkippedEvent.class, new ActionOnEventListener(ctx, new IncrementCounterAction(ctx, needCreationSkippedCounter)));

        //when a need is created (or it failed), decrement the halfCreatedNeed counter
        EventListener downCounter = new ActionOnEventListener(ctx, "downCounter",
                new DecrementCounterAction(ctx, creationUnfinishedCounter));
        //count a successful need creation
        bus.subscribe(CreateNeedCommandSuccessEvent.class, downCounter);
        //if a creation failed, we don't want to keep us from keeping the correct count
        bus.subscribe(CreateNeedCommandFailureEvent.class, downCounter);

        //trigger statistics logging about need generation
        BotTrigger needCreationStatsLoggingTrigger = new BotTrigger(ctx, Duration.ofSeconds(10));
        needCreationStatsLoggingTrigger.activate();
        bus.subscribe(StartCreatingNeedsCommandEvent.class, new ActionOnFirstEventListener(ctx, new PublishEventAction(ctx, new StartBotTriggerCommandEvent(needCreationStatsLoggingTrigger))));
        //just do some logging when a need is created
        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, "creationStatsLogger", needCreationStatsLoggingTrigger, new BaseEventBotAction(ctx) {
            volatile int lastOutput = 0;
            volatile long lastOutputMillis = System.currentTimeMillis();

            @Override
            protected void doRun(final Event event, EventListener executingListener) throws Exception {
                int cnt = needCreationStartedCounter.getCount();
                long now = System.currentTimeMillis();
                long millisSinceLastOutput = now-lastOutputMillis;
                int countSinceLastOutput = cnt-lastOutput;
                double countPerSecond = (double) countSinceLastOutput / ((double) millisSinceLastOutput / 1000d);
                logger.info("progress on need creation: total:{}, successful: {}, failed: {}, still waiting for response: {}, skipped: {}, messages inflight: {}, create trigger interval: {} millis, millis since last output: {}, started since last ouput: {} ({} per second)",
                        new Object[]{cnt,
                                needCreationSuccessfulCounter.getCount(),
                                needCreationFailedCounter.getCount(),
                                creationUnfinishedCounter.getCount(),
                                needCreationSkippedCounter.getCount(),
                                messagesInflightCounter.getCount(),
                                createNeedTrigger.getInterval().toMillis(),
                                millisSinceLastOutput,
                                countSinceLastOutput,
                                String.format("%.2f", countPerSecond)});
                lastOutput = cnt;
                lastOutputMillis = now;
            }
        }));

        //stop the stats logging trigger when need generation is finished
        bus.subscribe(NeedGenerationFinishedEvent.class, new ActionOnFirstEventListener(ctx, new PublishEventAction(ctx, new StopBotTriggerCommandEvent(needCreationStatsLoggingTrigger))));

        //when the needproducer is exhausted, we have to wait until all unfinished need creations finish
        //when they do, the NeedGenerationFinishedEvent is published
        bus.subscribe(NeedProducerExhaustedEvent.class, new ActionOnFirstEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
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
        getEventBus().subscribe(NeedProducerExhaustedEvent.class, new ActionOnFirstEventListener(ctx,
                new PublishEventAction(ctx, new StopBotTriggerCommandEvent(createNeedTrigger))));

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

        //wait for two things: need creation to finish and the connection info to be available,
        //then publish the StartCreatingConnectionsCommandEvent
        EventListener connectionCreationStarter = new ActionOnceAfterNEventsListener(ctx, 2, new PublishEventAction(ctx, new StartCreatingConnectionsCommandEvent()));
        bus.subscribe(NeedGenerationFinishedEvent.class, connectionCreationStarter);
        bus.subscribe(ConnectionProducerInitializedEvent.class, connectionCreationStarter);


        BotTrigger createConnectionsTrigger = new BotTrigger(ctx, Duration.ofMillis(100));
        createConnectionsTrigger.activate();
        bus.subscribe(StartCreatingConnectionsCommandEvent.class, new ActionOnFirstEventListener(ctx, new PublishEventAction(ctx, new StartBotTriggerCommandEvent(createConnectionsTrigger))));
        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, createConnectionsTrigger, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                if (isTooManyMessagesInflight(messagesInflightCounter)){
                    return;
                }
                adjustTriggerInterval(createConnectionsTrigger, messagesInflightCounter);


                //create connection
                Iterator<ConnectionToCreate> connectionToCreateIterator = connectionToCreateIteratorWrapper[0];
                if (connectionToCreateIterator == null) return;

                ConnectionToCreate connectionToCreate = null;
                String ownNeedUriString = null;
                String remoteNeedUriString = null;
                while (connectionToCreate == null) {
                    connectionToCreate = connectionToCreateIterator.next();
                    if (connectionToCreate == null) {
                        bus.publish(new ConectionProducerExhaustedEvent());
                        bus.unsubscribe(executingListener);
                        connectionToCreateIteratorWrapper[0] = null;
                        return;
                    }

                    ownNeedUriString = (String) getBotContextWrapper().getBotContext().loadFromObjectMap(NAME_INTERNAL_ID_TO_NEEDS, connectionToCreate.getInternalIdFrom().getURI().toString());
                    remoteNeedUriString = (String) getBotContextWrapper().getBotContext().loadFromObjectMap(NAME_INTERNAL_ID_TO_NEEDS, connectionToCreate.getInternalIdTo().getURI().toString());
                    List<Object> processedConnections = getBotContextWrapper().getBotContext().loadFromListMap(NAME_PROCESSED_CONNECTIONS, ownNeedUriString);
                    if (processedConnections.contains(remoteNeedUriString)){
                        //we've already processed this connection in an earlier run of the bot
                        bus.publish(new ConnectionCreationSkippedEvent());
                        return;
                    }
                    if (ownNeedUriString == null) {
                        logger.debug("cannot make connection from internal id {} because mapping to published need URI is missing", connectionToCreate.getInternalIdFrom());
                        connectionToCreate = null;
                    } else if (remoteNeedUriString == null) {
                        logger.debug("cannot make connection to internal id {} because mapping to published need URI is missing", connectionToCreate.getInternalIdTo());
                        connectionToCreate = null;
                    }
                }

                if (connectionToCreate != null) {
                    URI ownNeedURI = URI.create(ownNeedUriString);
                    URI remoteNeedURI = URI.create(remoteNeedUriString);
                    ConnectCommandEvent command = new ConnectCommandEvent(ownNeedURI, remoteNeedURI);
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
                        connectCommandEvent.getRemoteNeedURI().toString(),
                        connectCommandEvent.getNeedURI().toString());
            }
        }));



        //if the ConnectCommand fails, we have to remove the association between the two needs because the
        //connect we're waiting for will never happen
        bus.subscribe(ConnectCommandFailureEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx){
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                ConnectCommandFailureEvent connectCommandFailureEvent = (ConnectCommandFailureEvent) event;
                ConnectCommandEvent connectCommandEvent = (ConnectCommandEvent) connectCommandFailureEvent.getOriginalCommandEvent();
                getBotContextWrapper().getBotContext().removeFromListMap(NAME_EXPECTED_INCOMING_CONNECT,
                        connectCommandEvent.getRemoteNeedURI().toString(),
                        connectCommandEvent.getNeedURI().toString());
            }
        }));



        //if we receive a connect message, we have to check if it's an expected one
        //if so, accept and remove the association from our botContext.
        bus.subscribe(ConnectFromOtherNeedEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                ConnectFromOtherNeedEvent connectEvent = (ConnectFromOtherNeedEvent) event;
                List<Object> expectedUriStrings = getBotContextWrapper().getBotContext().loadFromListMap(NAME_EXPECTED_INCOMING_CONNECT, connectEvent.getNeedURI().toString());
                if (expectedUriStrings == null) {
                    logger.debug("ignoring connect received on behalf of need {} from remote need {} because we're not expecting to be contacted by that need.", connectEvent.getNeedURI(), connectEvent.getRemoteNeedURI());
                    return;
                }
                if (! expectedUriStrings.contains(connectEvent.getRemoteNeedURI().toString())) {
                    logger.debug("ignoring connect received on behalf of need {} from remote need {} because we're not expecting to be contacted by that need", connectEvent.getNeedURI(), connectEvent.getRemoteNeedURI());
                    return;
                }
                Connection con = connectEvent.getCon();
                bus.publish(new OpenCommandEvent(con));
                getBotContextWrapper().getBotContext().removeFromListMap(NAME_EXPECTED_INCOMING_CONNECT,
                        connectEvent.getRemoteNeedURI().toString(),
                        connectEvent.getNeedURI().toString());
                }
            }
        ));

        //we send the feedback after we've received the open command from the need we connected to
        bus.subscribe(OpenFromOtherNeedEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                Connection con = ((OpenFromOtherNeedEvent)event).getCon();
                bus.publish(new FeedbackCommandEvent(con, con.getConnectionURI(), URI.create(WON.HAS_BINARY_RATING.getURI()), URI.create(WON.GOOD.getURI())));
            }
        }));

        //if the feedback command succeeds, the workflow is finished for that connection. We have to remember that we
        // made the association, so we don't try it the next time the bot runs
        bus.subscribe(FeedbackCommandSuccessEvent.class, new ActionOnEventListener(ctx, new BaseEventBotAction(ctx) {
            @Override
            protected void doRun(Event event, EventListener executingListener) throws Exception {
                FeedbackCommandSuccessEvent feedbackCommandSuccessEvent = (FeedbackCommandSuccessEvent) event;
                getBotContextWrapper().getBotContext().addToListMap(NAME_PROCESSED_CONNECTIONS,
                        feedbackCommandSuccessEvent.getNeedURI().toString(),
                        feedbackCommandSuccessEvent.getRemoteNeedURI().toString());
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
                logger.info("progress on connection creation: connects sent: {} finished: {}, opens finished: {}, feedbacks finished: {}, skipped: {}, inflight {}, trigger interval: {} millis, messages sent: {}, millis since last output: {}, started since last ouput: {} ({} per second)",
                            new Object[]{ connectCommandCounter.getCount(),
                                    connectCommandResultCounter.getCount(),
                                    openCommandResultCounter.getCount(),
                                    feedbackCommandResultCounter.getCount(),
                                    connectSkippedCounter.getCount(),
                                    messagesInflightCounter.getCount(),
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
        BotTrigger statisticsLoggingTrigger = new BotTrigger(ctx, Duration.ofMinutes(10));
        statisticsLoggingTrigger.activate();
        bus.subscribe(BotTriggerEvent.class, new ActionOnTriggerEventListener(ctx, "statslogger", statisticsLoggingTrigger, new StatisticsLoggingAction(ctx)));
        bus.publish(new StartBotTriggerCommandEvent(statisticsLoggingTrigger));

        if (skipNeedCreation) {
            bus.publish(new StartCreatingConnectionsCommandEvent());
        } else {
            bus.publish(new StartCreatingNeedsCommandEvent());
        }
    }

    private boolean isTooManyMessagesInflight(Counter messagesInflightCounter) {
        //cut off if too many messages are already in-flight
        if (messagesInflightCounter.getCount() > maxInflightCount) {
            return true;
        }
        return false;
    }

    private void adjustTriggerInterval(BotTrigger createConnectionsTrigger, Counter targetCounter) {
        //change interval to achieve desired inflight count
        int desiredInflightCount = targetInflightCount;
        int inflightCountDiff = targetCounter.getCount() - desiredInflightCount;
        double factor = (double) inflightCountDiff / (double)desiredInflightCount;
        createConnectionsTrigger.changeIntervalByFactor(1 + 0.001 * factor);
    }


    public void setConnectionToCreateProducer(ConnectionToCreateProducer connectionToCreateProducer) {
        this.connectionToCreateProducer = connectionToCreateProducer;
    }
}
