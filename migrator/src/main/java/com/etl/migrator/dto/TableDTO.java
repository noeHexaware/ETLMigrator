package com.etl.migrator.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter @Setter
@RequiredArgsConstructor
public class TableDTO {
    private String fromTable;
    private String fromIdKey;
    private String toTable;
    private String foreignKey;
    private String db;
    private String migrationMode;
    private List<String> tables;
}
