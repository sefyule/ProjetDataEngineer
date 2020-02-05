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
        try(Connection connection = DriverManager.getConnection("jdbc:postgresql://sqletud.u-pem.fr/ychekiri_db", "ychekiri", "01/02/1961")){
            Statement statement = null;
            try {
                statement = ((Connection) connection).createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            ResultSet resultSet = null;
            ResultSet resultSet2 = null;
            try {
                resultSet = statement.executeQuery("select * from drugs4projet ORDER BY RANDOM()  LIMIT 1");
                resultSet2 = statement.executeQuery("select * from pharm4projet ORDER BY RANDOM()  LIMIT 1");
            } catch (SQLException e) {
                e.printStackTrace();
            }


            while (resultSet.next()) {
                try {
                    refProduit=resultSet.getInt("cip");
                    prix=resultSet.getDouble("prix");


                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }


            while (resultSet2.next()) {
                try {
                    refLieuAchat=resultSet.getInt("id");



                } catch (SQLException e) {
                    e.printStackTrace();
                }
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
