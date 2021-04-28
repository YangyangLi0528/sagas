package com.lyyco.sagas.orchestration;

import io.eventuate.tram.messaging.common.Message;

public interface SagaDefinition<Data> {
    SagaActions<Data> start(Data sagaData);

    SagaActions<Data> handleReply(String currentState, Data sagaData, Message message);
}
