package tp3;

import java.sql.*;

public class Pharmacy {
    private int id;
    private String nom;
    private String adresse;
    private String depart;
    private String region;


    public Pharmacy() throws SQLException {
        // Connection parameters
        final String URL = "jdbc:mysql://remotemysql.com:3306/CgOJWRXTYv";
        final String LOGIN = "CgOJWRXTYv";
        final String PASSWORD = "jjothkqy4a";

        // Try-with-resources => No need to close the connection
        try(Connection connection = DriverManager.getConnection(URL, LOGIN, PASSWORD)) {
            //System.out.println ("Database connection established");
            Statement statement = null;
            try {
                statement = ((Connection) connection).createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {

                ResultSet  resultSet = statement.executeQuery("select * from pharm4projet ORDER BY RAND()  LIMIT 1");

                while (resultSet.next()) {
                    try {
                        id =resultSet.getInt("id");
                        nom=resultSet.getString("nom");
                        adresse=resultSet.getString("adresse");
                        depart=resultSet.getString("depart");
                        region=resultSet.getString("region");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public Pharmacy(int id, String nom, String adresse, String depart, String region) {
        this.id = id;
        this.nom = nom;
        this.adresse = adresse;
        this.depart = depart;
        this.region = region;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNom() {
        return nom;
    }

    public void setNom(String nom) {
        this.nom = nom;
    }

    public String getAdresse() {
        return adresse;
    }

    public void setAdresse(String adresse) {
        this.adresse = adresse;
    }

    public String getDepart() {
        return depart;
    }

    public void setDepart(String depart) {
        this.depart = depart;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "Pharmacy{" +
                "id=" + id +
                ", nom='" + nom + '\'' +
                ", adresse='" + adresse + '\'' +
                ", depart='" + depart + '\'' +
                ", region='" + region + '\'' +
                '}';
    }
}
