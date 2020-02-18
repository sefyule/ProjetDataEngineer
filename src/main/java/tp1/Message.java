package tp1;

import java.sql.*;

public class Message {


    private String nom;
    private String prenom;
    private int cip;
    private double prix;
    private int idpharma;


    public Message() throws SQLException {
        Personne p =new Personne();
        this.nom=p.getLastName();
        this.prenom=p.getFirstName();
        try(Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost/M2-BigData", "postgres", "hersant")){
            Statement statement = null;
            try {
                statement = ((Connection) connection).createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                ResultSet resultSet = statement.executeQuery("select * from drugs4projet ORDER BY RANDOM()  LIMIT 1");
                while (resultSet.next()) {
                    try {
                        cip =resultSet.getInt("cip");
                        int randPOurcentage = 10 - (int)(Math.random() * 21);
                        double tmpPrix = resultSet.getDouble("prix");
                        prix = tmpPrix + (tmpPrix*randPOurcentage/ 100);


                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

                ResultSet  resultSet2 = statement.executeQuery("select * from pharm4projet ORDER BY RANDOM()  LIMIT 1");

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
        }


    }


    public String getNom() {
        return nom;
    }

    public String getPrenom() {
        return prenom;
    }

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
                ", refProduit=" + cip +
                ", prix=" + prix +
                ", refLieuAchat=" + idpharma +
                '}';
    }



}
