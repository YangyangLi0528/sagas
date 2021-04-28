package com.lyyco.sagas.orchestration;

import com.lyyco.sagas.common.LockTarget;
import com.lyyco.sagas.common.SagaCommandHeaders;
import com.lyyco.sagas.common.SagaReplyHeaders;
import com.lyyco.sagas.common.SagaUnlockCommand;
import io.eventuate.tram.commands.common.*;
import io.eventuate.tram.commands.producer.CommandProducer;
import io.eventuate.tram.messaging.common.Message;
import io.eventuate.tram.messaging.consumer.MessageConsumer;
import io.eventuate.tram.messaging.producer.MessageBuilder;

import java.util.*;

import static java.util.Collections.singleton;

public class SagaManagerImpl<Data> implements SagaManager<Data> {
    private Saga<Data> saga;
    private SagaInstanceRepository sagaInstanceRepository;
    private CommandProducer commandProducer;
    private MessageConsumer messageConsumer;
    private SagaLockManager sagaLockManager;
    private SagaCommandProducer sagaCommandProducer;

    public SagaManagerImpl(Saga<Data> saga,
                           SagaInstanceRepository sagaInstanceRepository,
                           CommandProducer commandProducer,
                           MessageConsumer messageConsumer,
                           SagaLockManager sagaLockManager,
                           SagaCommandProducer sagaCommandProducer) {
        this.saga = saga;
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.commandProducer = commandProducer;
        this.messageConsumer = messageConsumer;
        this.sagaLockManager = sagaLockManager;
        this.sagaCommandProducer = sagaCommandProducer;
    }

    public void setSagaInstanceRepository(SagaInstanceRepository sagaInstanceRepository) {
        this.sagaInstanceRepository = sagaInstanceRepository;
    }

    public void setCommandProducer(CommandProducer commandProducer) {
        this.commandProducer = commandProducer;
    }

    public void setMessageConsumer(MessageConsumer messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    public void setSagaLockManager(SagaLockManager sagaLockManager) {
        this.sagaLockManager = sagaLockManager;
    }

    public void setSagaCommandProducer(SagaCommandProducer sagaCommandProducer) {
        this.sagaCommandProducer = sagaCommandProducer;
    }

    @Override
    public SagaInstance create(Data sagaData) {
        return create(sagaData, Optional.empty());
    }

    @Override
    public void subscribeToReplyChannel() {
        messageConsumer.subscribe(saga.getSagaType() + "-consumer", singleton(makeSagaReplyChannel()),
                this::handleMessage);
    }

    private void handleMessage(Message message) {
        if (message.hasHeader(SagaReplyHeaders.REPLY_SAGA_ID)) {
            handleReply(message);
        } else {

        }
    }

    private void handleReply(Message message) {
        if (!isReplyForThisSagaType(message)) {
            return;
        }
        String sagaId = message.getRequiredHeader(SagaReplyHeaders.REPLY_SAGA_ID);
        String sagaType = message.getRequiredHeader(SagaReplyHeaders.REPLY_SAGA_TYPE);
        SagaInstance sagaInstance = sagaInstanceRepository.find(sagaType, sagaId);
        Data sagaData = SagaDataSerde.deserializeSagaData(sagaInstance.getSerializedSagaData());
        message.getHeader(SagaReplyHeaders.REPLY_LOCKED).ifPresent(lockedTarget -> {
            String destination = message.getRequiredHeader(CommandMessageHeaders.inReply(CommandMessageHeaders.DESTINATION));
            sagaInstance.addDestinationsAndResources(singleton(new DestinationAndResource(destination, lockedTarget)));
        });
        String currentState = sagaInstance.getStateName();
        SagaActions<Data> actions = getStateDefinition().handleReply(currentState, sagaData, message);
        processActions(sagaId, sagaInstance, sagaData, actions);
    }

    @Override
    public SagaInstance create(Data sagaData, Optional<String> resource) {
        SagaInstance sagaInstance = new SagaInstance(getSagaType(),
                null,
                "",
                null,
                SagaDataSerde.serializeSagaData(sagaData),
                new HashSet<>());
        sagaInstanceRepository.save(sagaInstance);
        String sagaId = sagaInstance.getId();
        saga.onStarting(sagaId, sagaData);
        resource.ifPresent(r -> {
            if (!sagaLockManager.claimLock(getSagaType(), sagaId, r)) {
                throw new RuntimeException();
            }
        });
        SagaActions<Data> actions = getStateDefinition().start(sagaData);
        actions.getLocalException().ifPresent(e -> {
            throw e;
        });
        processActions(sagaId, sagaInstance, sagaData, actions);
        return sagaInstance;
    }

    private void processActions(String sagaId, SagaInstance sagaInstance, Data sagaData, SagaActions<Data> actions) {
        while (true) {
            if (actions.getLocalException().isPresent()) {
                actions = getStateDefinition().handleReply(actions.getUpdatedState().get(), actions.getUpdatedSagaData().get(), MessageBuilder.withPayload("{}")
                        .withHeader(ReplyMessageHeaders.REPLY_OUTCOME, CommandReplyOutcome.FAILURE.name())
                        .withHeader(ReplyMessageHeaders.REPLY_TYPE, Failure.class.getName()).build());
            } else {
                // only do this if successful

                String lastRequestId = sagaCommandProducer.sendCommands(this.getSagaType(), sagaId, actions.getCommands(), this.makeSagaReplyChannel());
                sagaInstance.setLastRequestId(lastRequestId);

                updateState(sagaInstance, actions);

                sagaInstance.setSerializedSagaData(SagaDataSerde.serializeSagaData(actions.getUpdatedSagaData().orElse(sagaData)));

                if (actions.isEndState()) {
                    performEndStateActions(sagaId, sagaInstance, actions.isCompensating(), sagaData);
                }

                sagaInstanceRepository.update(sagaInstance);

                if (!actions.isLocal())
                    break;

                actions = getStateDefinition().handleReply(actions.getUpdatedState().get(), actions.getUpdatedSagaData().get(), MessageBuilder
                        .withPayload("{}")
                        .withHeader(ReplyMessageHeaders.REPLY_OUTCOME, CommandReplyOutcome.SUCCESS.name())
                        .withHeader(ReplyMessageHeaders.REPLY_TYPE, Success.class.getName())
                        .build());
            }
        }
    }

    @Override
    public SagaInstance create(Data data, Class targetClass, Object targetId) {
        return create(data, Optional.of(new LockTarget(targetClass, targetId).getTarget()));
    }

    private String getSagaType() {
        return saga.getSagaType();
    }

    private SagaDefinition<Data> getStateDefinition() {
        SagaDefinition<Data> sm = saga.getSagaDefinition();

        if (sm == null) {
            throw new RuntimeException("state machine cannot be null");
        }

        return sm;
    }

    private void updateState(SagaInstance sagaInstance, SagaActions<Data> actions) {
        actions.getUpdatedState().ifPresent(stateName -> {
            sagaInstance.setStateName(stateName);
            sagaInstance.setEndState(actions.isEndState());
            sagaInstance.setCompensating(actions.isCompensating());
        });
    }


    private Boolean isReplyForThisSagaType(Message message) {
        return message.getHeader(SagaReplyHeaders.REPLY_SAGA_TYPE).map(x -> x.equals(getSagaType())).orElse(false);
    }

    private String makeSagaReplyChannel() {
        return getSagaType() + "-reply";
    }

    private void performEndStateActions(String sagaId, SagaInstance sagaInstance, boolean compensating, Data sagaData) {
        for (DestinationAndResource dr : sagaInstance.getDestinationsAndResources()) {
            Map<String, String> headers = new HashMap<>();
            headers.put(SagaCommandHeaders.SAGA_ID, sagaId);
            headers.put(SagaCommandHeaders.SAGA_TYPE, getSagaType());
            commandProducer.send(dr.getDestination(), dr.getResource(), new SagaUnlockCommand(), makeSagaReplyChannel(), headers);
        }

        if (compensating)
            saga.onSagaRolledBack(sagaId, sagaData);
        else
            saga.onSagaCompletedSuccessfully(sagaId, sagaData);

    }

}
