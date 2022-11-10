package com.etl.migrator.api;

import com.etl.migrator.dto.TableDTO;
import com.etl.migrator.service.MigratorService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.etl.migrator.queueConfig.MessageProducer;
import java.sql.SQLException;
import java.time.LocalDate;

@RestController
@RequestMapping("api/v1")
@RequiredArgsConstructor
public class MigratorController {

    @Autowired
    private ApplicationContext context;

    // @Autowired
    private final MigratorService migratorService;


    @GetMapping("hello")
    public String hello(){
        return "hello " + LocalDate.now();
    }

    @GetMapping("process")
    public ResponseEntity<Object> processMigration() throws SQLException{
    	//MigratorService migratorService = new MigratorService();
        return ResponseEntity.ok(migratorService.processMigration());
    }

    @GetMapping("getCollection")
    public ResponseEntity<Object> processCollection() throws SQLException {
    	//MigratorService migratorService = new MigratorService();
        return ResponseEntity.ok(migratorService.getCollection("department","idDepartment", "employee","department"));
    }

//    @GetMapping("makeCollection")
//    public ResponseEntity<Object> makeCollection() throws SQLException {
//        MigratorService migratorService = new MigratorService();
//        return ResponseEntity.ok(migratorService.makeCollection("department","idDepartment", "employee","department"));
//    }

    @PostMapping("makeCollectionsParams")
    public  ResponseEntity<Object> makeCollectionParams(@RequestBody TableDTO tableParams) throws SQLException {
        // MigratorService migratorService = new MigratorService();
        String data = migratorService.makeCollection(tableParams);

        MessageProducer producer = context.getBean(MessageProducer.class);
        //JSONParser parser = new JSONParser();
        //JSONObject json = (JSONObject) parser.parse(data);
        producer.sendMessage(data);

        return ResponseEntity.ok(data);
    }
}
