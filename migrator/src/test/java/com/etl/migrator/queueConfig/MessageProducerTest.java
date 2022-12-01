package com.etl.migrator.queueConfig;

import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class MessageProducerTest {

    @Mock
    MessageProducer messageProducer;

    @BeforeEach
    void setUp() {
        //messageProducer = new MessageProducer();
    }

    @Test
    public void testSendMessage(){
        // MessageProducer messageProducer1 = mock(MessageProducer.class);
        messageProducer = mock(MessageProducer.class);
        String message = "[{masterPk=idDepartment, name=IT, idDepartment=1, childrenName=employee, description=Informatic Techonologies, collection=migrator}, {masterPk=idDepartment, name=IT, idDepartment=1, childrenName=employee, description=Informatic Techonologies, collection=migrator}, {masterPk=idDepartment, name=HR, idDepartment=2, childrenName=employee, description=Human Resources, collection=migrator}, {masterPk=idDepartment, name=PM, idDepartment=3, childrenName=employee, description=Project Manager, collection=migrator}]";

        doNothing().when(messageProducer).sendMessage(message);
        verify(messageProducer, times(1)).sendMessage(message);
    }
}