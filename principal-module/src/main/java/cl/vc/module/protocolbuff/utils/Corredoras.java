package cl.vc.module.protocolbuff.utils;

import java.util.HashMap;

public class Corredoras {

   public static HashMap<String, String> getAll(){

       HashMap<String, String> allbrokercode = new HashMap<>();

       allbrokercode.put("035", "LVC");
       allbrokercode.put("070", "BTG");
       allbrokercode.put("086", "BCH");
       allbrokercode.put("066", "CDC");
       allbrokercode.put("085", "SCO");
       allbrokercode.put("082", "BIC");
       allbrokercode.put("058", "BCI");

       allbrokercode.put("051", "NVS");
       allbrokercode.put("043", "ITA");
       allbrokercode.put("041", "VCC");
       allbrokercode.put("069", "FYN");
       allbrokercode.put("088", "SAN");
       allbrokercode.put("042", "SUR");
       allbrokercode.put("090", "CON");
       allbrokercode.put("004", "RT4");

       allbrokercode.put("057", "NEV");
       allbrokercode.put("059", "TAN");
       allbrokercode.put("039", "VTT");
       allbrokercode.put("020", "SEC");
       allbrokercode.put("046", "JPM");
       allbrokercode.put("054", "EST");
       allbrokercode.put("062", "MBI");

       allbrokercode.put("076", "EUR");
       allbrokercode.put("032", "DUP");
       allbrokercode.put("061", "MER");

       allbrokercode.put("967", "VCC");
       allbrokercode.put("995", "VCC");
       allbrokercode.put("977", "VCC");
       allbrokercode.put("971", "VCC");
       allbrokercode.put("553", "VCC");
       allbrokercode.put("939", "VCC");
       allbrokercode.put("975", "VCC");
       allbrokercode.put("9713", "VCC");
       allbrokercode.put("945", "VCC");

       return allbrokercode;
   }
}
