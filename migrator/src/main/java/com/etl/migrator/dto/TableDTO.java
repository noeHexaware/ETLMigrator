package com.etl.migrator.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter @Setter
@RequiredArgsConstructor
public class TableDTO {
    private String fromTable;
    private String fromIdKey;
    private String toTable;
    private String foreignKey;

}
