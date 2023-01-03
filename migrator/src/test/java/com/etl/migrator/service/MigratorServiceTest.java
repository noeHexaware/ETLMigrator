package com.etl.migrator.service;

import com.etl.migrator.dto.TableDTO;
import org.json.simple.parser.ParseException;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.mock;


@ExtendWith(MockitoExtension.class)
public class MigratorServiceTest {

    MigratorService migratorService;

    @Mock
    private Connection connection;
    @Mock
    private ApplicationContext context;


    @BeforeEach
    void setUp() throws SQLException {
        migratorService =  mock(MigratorService.class);
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/",
                "root", "developer345");
    }

    @Disabled
    @Test
    public void testMakeCollection() throws SQLException, ParseException {
        // MigratorService migratorService1 = mock(MigratorService.class);
        TableDTO tableDTO = new TableDTO();
        tableDTO.setDb("migrator");
        tableDTO.setFromTable("department");
        tableDTO.setFromIdKey("idDepartment");
        tableDTO.setToTable("employee");
        tableDTO.setForeignKey("department");


        migratorService = new MigratorService(context);
        String collectionsResult = migratorService.makeCollection(tableDTO);
        String expected = "[{masterPk=idDepartment, name=IT, idDepartment=1, childrenName=employee, description=Informatic Techonologies, collection=migrator}, {masterPk=idDepartment, name=IT, idDepartment=1, childrenName=employee, description=Informatic Techonologies, collection=migrator}, {masterPk=idDepartment, name=HR, idDepartment=2, childrenName=employee, description=Human Resources, collection=migrator}, {masterPk=idDepartment, name=PM, idDepartment=3, childrenName=employee, description=Project Manager, collection=migrator}]";

        Assertions.assertEquals(expected, collectionsResult);
    }

    @Test
    public void testGetListColumns() throws SQLException {
        migratorService = new MigratorService(context);

        List<String> listResult = migratorService.getListColumns("migrator", "department");
        List<String> listExpected = Arrays.asList("idDepartment", "name", "description");

        Assertions.assertEquals(listExpected, listResult);
    }

    @Test
    public void testExtractValues() throws SQLException {
        migratorService = new MigratorService(context);

        Properties pojoSon = new Properties();
        pojoSon.put("idemployee", "1");
        pojoSon.put("lastname", "Rocha");
        pojoSon.put("salary", "1000");
        pojoSon.put("birthday", "1990-02-10");
        pojoSon.put("name", "Irving");
        pojoSon.put("department", "1");

        String result = migratorService.extractValues(pojoSon);
        String expected = "\"idemployee\":\"1\",\"birthday\":\"1990-02-10\",\"salary\":\"1000\",\"lastname\":\"Rocha\",\"department\":\"1\",\"name\":\"Irving\",";

        Assertions.assertEquals(expected, result);
    }
}