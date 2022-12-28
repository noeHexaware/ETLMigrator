package com.etl.migrator.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter
@AllArgsConstructor
public class ManyTableDTO {
    private String foreignKey;
    private String primaryTable;
    private String primaryKey;
}
