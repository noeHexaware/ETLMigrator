package com.etl.migrator.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter @Setter
@RequiredArgsConstructor
public class TwoInvertTablesDTO {
	private String mainTable;
    private String primKey;
    private String forgKey;
    private String secondTable;
    private String primaryKey;
    private String db;
    private String migrationMode;
}
