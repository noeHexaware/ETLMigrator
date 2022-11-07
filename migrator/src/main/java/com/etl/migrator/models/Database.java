package com.etl.migrator.models;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter @Setter
@RequiredArgsConstructor
public class Database {
    private String url;
    private String username;
    private String password;
}
