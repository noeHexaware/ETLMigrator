package com.etl.migrator.api;

import com.etl.migrator.service.MigratorService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.time.LocalDate;

@RestController
@RequestMapping("api/v1")
@RequiredArgsConstructor
public class MigratorController {

    private MigratorService migratorService;

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

    @GetMapping("makeCollection")
    public ResponseEntity<Object> makeCollection() throws SQLException {
        MigratorService migratorService = new MigratorService();
        return ResponseEntity.ok(migratorService.makeCollection("department","idDepartment", "employee","department"));
    }
}
