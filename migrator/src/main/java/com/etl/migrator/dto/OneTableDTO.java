package com.etl.migrator.dto;


import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;


@Getter @Setter
@RequiredArgsConstructor
public class OneTableDTO {
    public OneTableDTO(String table, String database) {
		// TODO Auto-generated constructor stub
    	fromTable = table;
    	db = database;
	}
	private String fromTable;
    private String db;

}
