package tp1;

import java.sql.*;

public class Message {


    private String nom;
    private String prenom;
    private int cip;
    private double prix;
    private int idpharma;
    //private String adresse;
    //private int age;


    public Message() throws SQLException {
        Personne p = new Personne();
        nom = p.getLastName();
        prenom = p.getFirstName();
        //adresse = p.getAdresse();
        //age = p.getAge();

        // Connection parameters
        final String URL = "jdbc:mysql://remotemysql.com:3306/CgOJWRXTYv";
        final String LOGIN = "CgOJWRXTYv";
        final String PASSWORD = "jjothkqy4a";

        // Try-with-resources => No need to close the connection
        try(Connection connection = DriverManager.getConnection(URL, LOGIN, PASSWORD)) {
            System.out.println ("Database connection established");
            Statement statement = null;
            try {
                statement = ((Connection) connection).createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                ResultSet resultSet = statement.executeQuery("select * from CgOJWRXTYv.drugs4projet ORDER BY RAND()  LIMIT 1");
                while (resultSet.next()) {
                    try {
                        cip =resultSet.getInt("cip");
                        //int randPOurcentage = 10 - (int)(Math.random() * 21);
                        int randPOurcentage = 10 - (int)(Math.random() * 20);
                        double tmpPrix = resultSet.getDouble("prix");
                        prix = tmpPrix + (tmpPrix*randPOurcentage/ 100);


                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

                ResultSet  resultSet2 = statement.executeQuery("select * from pharm4projet ORDER BY RAND()  LIMIT 1");

                while (resultSet2.next()) {
                    try {
                        idpharma =resultSet2.getInt("id");

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


    public String getNom() {
        return nom;
    }

    public String getPrenom() {
        return prenom;
    }
    /*public String getAdresse() {
        return adresse;
    }
    public int getAge() { return age;}*/

    public int getCip() {
        return cip;
    }

    public double getPrix() {
        return prix;
    }

    public int getIdpharma() {
        return idpharma;
    }

    @Override
    public String toString() {
        return "tp1.Message{" +
                "nom='" + nom + '\'' +
                ", prenom='" + prenom + '\'' +
                //", adresse='" + adresse + '\'' +
                //", age='" + age + '\'' +
                ", refProduit=" + cip +
                ", prix=" + prix +
                ", refLieuAchat=" + idpharma +
                '}';
    }



}
