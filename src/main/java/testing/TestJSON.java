package testing;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;


public class TestJSON {
    public static void main(String args[]) throws IOException {
        JsonParser j = new JsonParser();
        JsonObject o = (JsonObject) j.parse(
                "{\"description\":\"disabled marine jake sully travels to planet pandora to become an avatar, ingratiate himself with the natives and help americans mine lucrative unobtainium. but he finds himself in an interstellar conflict after falling for na'vi warrior neytiri.\",\"genre\":\"action\",\"homepage\":\"http://www.avatarmovie.com/\",\"id\":345,\"imageurl\":\"http://cf1.imgobject.com/posters/374/4bd29ddd017a3c63e8000374/avatar-mid.jpg\",\"imdbid\":\"tt0499549\",\"language\":\"en\",\"lastmodified\":\"1300200001000\",\"releasedate\":\"1261090800000\",\"runtime\":162,\"studio\":\"twentieth century fox film corporation\",\"tagline\":\"enter the world of pandora\",\"title\":\"avatar\",\"trailer\":\"http://www.youtube.com/watch?v=avdo-cx-mca\",\"version\":1465,\"label\":\"movie\"}");
        System.out.println(o.get("description").toString());
    }
}
