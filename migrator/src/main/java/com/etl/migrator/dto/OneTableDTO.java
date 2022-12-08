package com.etl.migrator.dto;


import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;


@Getter @Setter
@RequiredArgsConstructor
public class OneTableDTO {
    private String fromTable;
    private String db;

}
