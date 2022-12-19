package com.etl.migrator.constants;

import java.util.ArrayList;

import org.json.simple.JSONObject;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter @Setter
@RequiredArgsConstructor
public class NestedDocTransformed {
	
	private String masterTable;
	private ArrayList<JSONObject> docs;
	
	public NestedDocTransformed(String fromTable, ArrayList<JSONObject> docs2) {
		masterTable = fromTable;
		docs = docs2;
	}
}
