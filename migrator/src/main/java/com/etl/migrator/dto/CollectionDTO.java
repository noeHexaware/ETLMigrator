package com.etl.migrator.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@RequiredArgsConstructor
public class CollectionDTO {
    private String database;
    private List<String> tables;
    private List<TableDTO> relational;
}
