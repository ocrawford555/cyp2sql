package production;

import database.DbUtil;
import org.apache.commons.lang3.SystemUtils;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.File;
import java.sql.SQLException;
import java.util.Properties;

class SendResultsEmail {
    static void sendEmail(String dbName) throws SQLException {
        String testResultContents = DbUtil.getTestResults(dbName);
        String email = "testneo4jcambridge@gmail.com";
        final String username = email;
        final String pw = "neo4j###73";

        Properties props = new Properties();
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(username, pw);
                    }
                });

        try {

            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(email));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse("ojc37@cam.ac.uk"));
            message.setSubject("Test Results Cyp2SQL -- " + dbName);

            Multipart multipart = new MimeMultipart();

            MimeBodyPart textBodyPart = new MimeBodyPart();
            textBodyPart.setText(testResultContents, "utf-8", "html");

            String file;
            if (SystemUtils.IS_OS_LINUX) {
                file = "/home/ojc37/props/testR.csv";
            } else {
                file = "C:/Users/ocraw/Desktop/testR.csv";
            }

            MimeBodyPart attachmentBodyPart = new MimeBodyPart();
            DataSource source = new FileDataSource(file); // ex : "C:\\test.pdf"
            attachmentBodyPart.setDataHandler(new DataHandler(source));
            attachmentBodyPart.setFileName("full_results.csv"); // ex : "test.pdf"

            multipart.addBodyPart(textBodyPart);  // add the text part
            multipart.addBodyPart(attachmentBodyPart); // add the attachment part

            message.setContent(multipart);

            System.out.println("Sending mail...");
            Transport.send(message);
            System.out.println("Sent!");
            File f = new File(file);
            f.delete();
        } catch (MessagingException e) {
            System.err.println("Failed to send...");
            throw new RuntimeException(e);
        }
    }
}
