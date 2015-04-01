package cz.cvut.fit.hrstkmir.midip.tools;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author mira
 */
public class CreateModelDatabase {

    private static class Person {

        private String ID;
        private String name;
        private String surname;
        private String sex;
        private String residence;
        private String district;
        private String country;

        public Person(String ID,
                String name,
                String surname,
                String sex,
                String residence,
                String district,
                String country) {
            this.ID = ID;
            this.name = name;
            this.surname = surname;
            this.sex = sex;
            this.residence = residence;
            this.district = district;
            this.country = country;
        }

        public Person() {

        }

        public String getID() {
            return ID;
        }

        public String getName() {
            return name;
        }

        public String getSurname() {
            return surname;
        }

        public String getSex() {
            return sex;
        }

        public void setID(String ID) {
            this.ID = ID;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setSurname(String surname) {
            this.surname = surname;
        }

        public void setSex(String sex) {
            this.sex = sex;
        }

        public void setResidence(String residence) {
            this.residence = residence;
        }

        public void setDistrict(String district) {
            this.district = district;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getResidence() {
            return residence;
        }

        public String getDistrict() {
            return district;
        }

        public String getCountry() {
            return country;
        }

        public void printPerson() {
            System.out.println(this.ID + ", " + this.name + ", " + this.surname + ", "
                    + this.sex + ", " + this.residence + ", " + this.district + ", " + this.country);
        }

    }

    private class City {

        private String residence;
        private String district;
        private String country;
        private Integer population;

        public City() {
        }

        public void setResidence(String residence) {
            this.residence = residence;
        }

        public void setDistrict(String district) {
            this.district = district;
        }

        public void setPopulation(Integer population) {
            this.population = population;
        }

        public Integer getPopulation() {
            return population;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getResidence() {
            return residence;
        }

        public String getDistrict() {
            return district;
        }

        public String getCountry() {
            return country;
        }

    }

    static List<City> cityList = new ArrayList<>();
    static Multimap<String, String> usedID = ArrayListMultimap.create();
    static List<String> MaleNames = new ArrayList<>();
    static List<String> FemaleNames = new ArrayList<>();
    static List<String> MaleSurnames = new ArrayList<>();
    static List<String> FemaleSurnames = new ArrayList<>();

    public void convertCsvToList() {
        String csvFileToRead = "data/obce.csv";
        BufferedReader br = null;
        String line = "";
        String splitBy = ",";

        try {
            boolean firstline = true;

            br = new BufferedReader(new FileReader(csvFileToRead));
            while ((line = br.readLine()) != null) {
                if (firstline) {
                    firstline = false;
                    continue;
                }

                // split on comma(',')  
                String[] cities = line.split(splitBy);

                // create car object to store values  
                City CityObject = new City();

                // add values from csv to car object   
                CityObject.setResidence(cities[0]);
                CityObject.setDistrict(cities[1]);
                CityObject.setCountry(cities[2]);
                CityObject.setPopulation(Integer.parseInt(cities[10]));

                // adding car objects to a list  
                cityList.add(CityObject);

            }
            // print values stored in carList  
            // printCityList(cityList);

        } catch (FileNotFoundException e) {
        } catch (IOException e) {
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public void makeListOfMaleNames() {
        String token1 = "";
        try (Scanner inFile1 = makeList("data/muzska-jmena.txt")) {
            while (inFile1.hasNext()) {
                token1 = inFile1.next();
                MaleNames.add(token1);
            }
            inFile1.close();
        }
    }

    public void makeListOfFemaleNames() {
        String token1 = "";
        try (Scanner inFile1 = makeList("data/zenska-jmena.txt")) {
            while (inFile1.hasNext()) {
                token1 = inFile1.next();
                FemaleNames.add(token1);
            }
            inFile1.close();
        }
    }

    public void makeListOfMaleSurnames() {
        String token1 = "";
        try (Scanner inFile1 = makeList("data/muzska-prijmeni.txt")) {
            while (inFile1.hasNext()) {
                token1 = inFile1.next();
                MaleSurnames.add(token1);
            }
            inFile1.close();
        }
    }

    public void makeListOfFemaleSurnames() {
        String token1 = "";
        try (Scanner inFile1 = makeList("data/zenska-prijmeni.txt")) {
            while (inFile1.hasNext()) {
                token1 = inFile1.next();
                FemaleSurnames.add(token1);
            }
            inFile1.close();
        }
    }

    private Scanner makeList(String file) {

        try {
            return new Scanner(new File(file)).useDelimiter("\\r?\\n");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(CreateModelDatabase.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static String getRandomMaleName() {
        return MaleNames.get(randBetween(0, MaleNames.size() - 1));
    }

    public static String getRandomMaleSurname() {
        return MaleNames.get(randBetween(0, MaleSurnames.size() - 1));
    }

    public static String getRandomFemaleName() {
        return FemaleNames.get(randBetween(0, 76));
    }

    public static String getRandomFemaleSurname() {
        return FemaleNames.get(randBetween(0, 76));
    }

    private static String generateID() {
        boolean verifyID = false;
        while (true) {
            GregorianCalendar gc = new GregorianCalendar();
            int year = randBetween(1900, 2010);
            gc.set(GregorianCalendar.YEAR, year);
            int dayOfYear = randBetween(1, gc.getActualMaximum(GregorianCalendar.DAY_OF_YEAR));
            gc.set(GregorianCalendar.DAY_OF_YEAR, dayOfYear);
            String yearID = String.valueOf(year).substring(2);
            String monthID = (gc.get(GregorianCalendar.MONTH) < 10) ? "0" + String.valueOf(gc.get(GregorianCalendar.MONTH)) : String.valueOf(gc.get(GregorianCalendar.MONTH));
            String dayID = (gc.get(GregorianCalendar.DAY_OF_MONTH) < 10) ? "0" + String.valueOf(gc.get(GregorianCalendar.DAY_OF_MONTH)) : String.valueOf(gc.get(GregorianCalendar.DAY_OF_MONTH));

            String date = yearID + monthID + dayID;

            if (usedID.containsKey(date)) {
                String lastDigits = String.valueOf(randBetween(1000, 9999));
                if (!usedID.get(date).contains(lastDigits)) {
                    usedID.put(date, lastDigits);
                    return date + lastDigits;
                }

            } else {
                String lastDigits = String.valueOf(randBetween(1000, 9999));
                usedID.put(date, lastDigits);
                return date + lastDigits;
            }

        }
    }

    public static int randBetween(int start, int end) {
        return start + (int) Math.round(Math.random() * (end - start));
    }

    public void printCityList(List<City> carListToPrint) {
        for (City carListToPrint1 : carListToPrint) {
            System.out.println("Mesto [jmeno= " + carListToPrint1.getResidence() + " , okres=" + carListToPrint1.getDistrict() + " , kraj=" + carListToPrint1.getCountry() + "]");
        }
    }

    public static void main(String[] args) throws IOException {
        List<Person> Person = new ArrayList<>();

        int setMaxPopulationInCity = 10;
        int setMaxGeneratedRows = 100;

        int maxPopulationInCity = 0;
        int maxGeneratedRows = 0;

        CreateModelDatabase model = new CreateModelDatabase();

        model.convertCsvToList();
        model.makeListOfMaleNames();
        model.makeListOfFemaleNames();
        model.makeListOfMaleSurnames();
        model.makeListOfFemaleSurnames();

        HBaseConfiguration hbaseConfig = new HBaseConfiguration();
        try (HTable htable = new HTable(hbaseConfig, "lide2")) {
            htable.setAutoFlush(false);
            htable.setWriteBufferSize(1024 * 1024 * 12);
            
            for (City city : cityList) {
                for (int i = 0; i < city.getPopulation(); i++) {
                    Person person = new Person();
                    person.setID(generateID());
                    person.setResidence(city.getResidence());
                    person.setDistrict(city.getDistrict());
                    person.setCountry(city.getCountry());
                    
                    Random randomno = new Random();
                    boolean male = randomno.nextBoolean();
                    
                    if (male) {
                        person.setName(getRandomMaleName());
                        person.setSurname(getRandomMaleSurname());
                        person.setSex("male");
                        
                    } else {
                        person.setName(getRandomFemaleName());
                        person.setSurname(getRandomFemaleSurname());
                        person.setSex("female");
                        int monthFemale = Integer.parseInt(String.valueOf(person.getID().charAt(2))) + 5;
                        person.setID(person.getID().substring(0, 2) + monthFemale + person.getID().substring(3));
                    }
                    
                    person.printPerson();
                    
                    Put put = new Put(Bytes.toBytes(person.getID()));
                    put.add(Bytes.toBytes("osoba"), Bytes.toBytes("jmeno"), Bytes.toBytes(person.getName()));
                    put.add(Bytes.toBytes("osoba"), Bytes.toBytes("prijmeni"), Bytes.toBytes(person.getSurname()));
                    put.add(Bytes.toBytes("osoba"), Bytes.toBytes("pohlavi"), Bytes.toBytes(person.getSex()));
                    put.add(Bytes.toBytes("osoba"), Bytes.toBytes("obec"), Bytes.toBytes(person.getResidence()));
                    put.add(Bytes.toBytes("osoba"), Bytes.toBytes("okres"), Bytes.toBytes(person.getDistrict()));
                    put.add(Bytes.toBytes("osoba"), Bytes.toBytes("kraj"), Bytes.toBytes(person.getCountry()));
                    htable.put(put);
                    maxPopulationInCity++;
                    maxGeneratedRows++;
                    if (maxPopulationInCity > setMaxPopulationInCity) {
                        break;
                    }
                    
                }
                if (maxGeneratedRows > setMaxGeneratedRows) {
                    break;
                }
            }
            
            htable.flushCommits();
        }
        System.out.println("done");
    }
}
