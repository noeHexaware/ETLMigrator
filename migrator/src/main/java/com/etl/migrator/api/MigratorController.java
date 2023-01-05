package com.etl.migrator.api;

import com.etl.migrator.dto.ManyTableDTO;
import com.etl.migrator.dto.OneTableDTO;
import com.etl.migrator.dto.TableDTO;
import com.etl.migrator.dto.TwoInvertTablesDTO;
import com.etl.migrator.dto.CollectionDTO;
import com.etl.migrator.service.MigratorService;
import lombok.RequiredArgsConstructor;

import org.json.simple.parser.ParseException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.sql.SQLException;
import java.time.LocalDate;

@RestController
@RequestMapping("api/v1")
@RequiredArgsConstructor
public class MigratorController {
    private final MigratorService migratorService;

    @GetMapping("statusApi")
    public String hello(){
        return "API up, date: " + LocalDate.now();
    }

    /**
     * Send database and table parameters to proccess the Migration
     * @param tableParams
     * @return
     * @throws SQLException
     * @throws ParseException 
     */
    @PostMapping("makeCollectionsParams")
    public  ResponseEntity<Object> makeCollectionParams(@RequestBody TableDTO tableParams) throws SQLException, ParseException {
        String data = migratorService.makeCollection(tableParams);
        return ResponseEntity.ok("Migration completed!");
    }
    
    @PostMapping("invertRelation")
    public  ResponseEntity<Object> invertRelation(@RequestBody TwoInvertTablesDTO tableParams) throws SQLException, ParseException {
        String data = migratorService.processTwoTables(tableParams);
        return ResponseEntity.ok("Migration completed!");
    }

    /**
     * Send database and table parameters to proccess the Migration
     * @param oneTableParams
     * @return
     * @throws SQLException
     */
    @PostMapping("makeCollectionsParamsOne")
    public  ResponseEntity<Object> makeCollectionParamsOne(@RequestBody OneTableDTO oneTableParams) throws SQLException {
        String data = migratorService.makeCollectionOneTable(oneTableParams);
        return ResponseEntity.ok("Migration completed!");
    }

    /**
     * Process Migration 2 or more tables
     * @param collectionDTO
     * @return
     * @throws SQLException
     */
    @PostMapping("processMigration")
    public ResponseEntity<Object> processMigrationTables(@RequestBody CollectionDTO collectionDTO) throws SQLException {
        String data = migratorService.processMigrationTables(collectionDTO);
        return ResponseEntity.ok("Migration completed!");
    }

    @PostMapping("processManyToMany")
    public ResponseEntity<Object> processManyToMany(@RequestBody CollectionDTO dto) throws SQLException {
        String data = migratorService.makeCollectionManyToMany(dto);
        return ResponseEntity.ok("Migration completed!");
    }
}
