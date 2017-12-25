package com.bluedon.utils;

import java.util.Iterator;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class JSONtoLowerTools {
	public static JSONObject transObject(JSONObject o1){
        JSONObject o2=new JSONObject();
         Iterator it = o1.keys();
            while (it.hasNext()) {
                String key = (String) it.next();
                Object object = o1.get(key);           
                o2.accumulate(key.toLowerCase(), object);               
            }
            return o2;
    }
    
}
