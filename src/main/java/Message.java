import java.sql.*;

public class Message {


    private String nom;
    private String prenom;
    private int refProduit;
    private double prix;

    private int refLieuAchat;


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
                        refProduit=resultSet.getInt("cip");
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
                        refLieuAchat=resultSet2.getInt("id");

                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }




        }


    }



    @Override
    public String toString() {
        return "Message{" +
                "nom='" + nom + '\'' +
                ", prenom='" + prenom + '\'' +
                ", refProduit=" + refProduit +
                ", prix=" + prix +
                ", refLieuAchat=" + refLieuAchat +
                '}';
    }

}
