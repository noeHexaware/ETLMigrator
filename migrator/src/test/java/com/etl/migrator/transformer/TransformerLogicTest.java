package com.etl.migrator.transformer;

import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TransformerLogicTest {

    TransformerLogic transformerLogic;
    @BeforeEach
    void setUp() {
        transformerLogic = Mockito.spy(new TransformerLogic());
    }

    @Test
    public void testTransformData(){
        transformerLogic = Mockito.spy(new TransformerLogic());
        String message = "[{masterPk=idDepartment, name=IT, idDepartment=1, childrenName=employee, description=Informatic Techonologies, collection=migrator}, {masterPk=idDepartment, name=IT, idDepartment=1, childrenName=employee, description=Informatic Techonologies, collection=migrator}, {masterPk=idDepartment, name=HR, idDepartment=2, childrenName=employee, description=Human Resources, collection=migrator}, {masterPk=idDepartment, name=PM, idDepartment=3, childrenName=employee, description=Project Manager, collection=migrator}]";

        // doNothing().when(transformerLogic).transformData(message);

        verify(transformerLogic).transformData(message);
    }

    @Test
    public void whenTransformData(){
        TransformerLogic transformerLogic1 = mock(TransformerLogic.class);
        String message = "[{masterPk=idDepartment, name=IT, idDepartment=1, childrenName=employee, description=Informatic Techonologies, collection=migrator}, {masterPk=idDepartment, name=IT, idDepartment=1, childrenName=employee, description=Informatic Techonologies, collection=migrator}, {masterPk=idDepartment, name=HR, idDepartment=2, childrenName=employee, description=Human Resources, collection=migrator}, {masterPk=idDepartment, name=PM, idDepartment=3, childrenName=employee, description=Project Manager, collection=migrator}]";

        doNothing().when(transformerLogic1).transformData(message);
        verify(transformerLogic1, times(2)).transformData(message);
    }
}