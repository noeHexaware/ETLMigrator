package com.etl.migrator.api;

import com.etl.migrator.dto.OneTableDTO;
import com.etl.migrator.dto.TableDTO;
import com.etl.migrator.service.MigratorService;
import lombok.RequiredArgsConstructor;
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
     */
    @PostMapping("makeCollectionsParams")
    public  ResponseEntity<Object> makeCollectionParams(@RequestBody TableDTO tableParams) throws SQLException {
        String data = migratorService.makeCollection(tableParams);
        return ResponseEntity.ok(data);
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
        return ResponseEntity.ok(data);
    }
}
