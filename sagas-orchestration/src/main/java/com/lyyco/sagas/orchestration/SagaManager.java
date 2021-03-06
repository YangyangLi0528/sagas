package com.lyyco.sagas.orchestration;

import java.util.Optional;

public interface SagaManager<Data> {
    SagaInstance create(Data sagaData);

    void subscribeToReplyChannel();

    SagaInstance create(Data sagaData, Optional<String> lockTarget);
    SagaInstance create(Data data,Class targetClass,Object targetId);

}
