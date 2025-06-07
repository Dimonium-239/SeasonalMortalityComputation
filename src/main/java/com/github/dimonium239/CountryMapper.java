package com.github.dimonium239;

import java.util.Map;
import java.util.HashMap;

public class CountryMapper {
    public static Map<String, String> isoAlpha2ToCountryName() {
        Map<String, String> map = new HashMap<>();

        map.put("EU27_2020", "EU");
        map.put("AL", "Albania");
        map.put("AD", "Andorra");
        map.put("AT", "Austria");
        map.put("BY", "Belarus");
        map.put("BE", "Belgium");
        map.put("BA", "Bosnia and Herzegovina");
        map.put("BG", "Bulgaria");
        map.put("HR", "Croatia");
        map.put("CY", "Cyprus");
        map.put("CZ", "Czech Republic");
        map.put("DK", "Denmark");
        map.put("EE", "Estonia");
        map.put("FI", "Finland");
        map.put("FR", "France");
        map.put("DE", "Germany");
        map.put("GR", "Greece");
        map.put("HU", "Hungary");
        map.put("IS", "Iceland");
        map.put("IE", "Ireland");
        map.put("IT", "Italy");
        map.put("LV", "Latvia");
        map.put("LI", "Liechtenstein");
        map.put("LT", "Lithuania");
        map.put("LU", "Luxembourg");
        map.put("MT", "Malta");
        map.put("MD", "Moldova");
        map.put("MC", "Monaco");
        map.put("ME", "Montenegro");
        map.put("NL", "Netherlands");
        map.put("MK", "North Macedonia");
        map.put("NO", "Norway");
        map.put("PL", "Poland");
        map.put("PT", "Portugal");
        map.put("RO", "Romania");
        map.put("RU", "Russia");
        map.put("SM", "San Marino");
        map.put("RS", "Serbia");
        map.put("SK", "Slovakia");
        map.put("SI", "Slovenia");
        map.put("ES", "Spain");
        map.put("SE", "Sweden");
        map.put("CH", "Switzerland");
        map.put("TR", "Turkey");
        map.put("UA", "Ukraine");
        map.put("GB", "United Kingdom");
        map.put("VA", "Vatican City");

        return map;
    }
}
