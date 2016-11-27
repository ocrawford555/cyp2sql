package translator;

import java.util.UUID;

public class GenerateAlias {
    public static String gen() {
        String uuid = String.valueOf(UUID.randomUUID());
        return uuid.replace("-", "_").replace(String.valueOf(uuid.charAt(0)), "dB").substring(0,9);
    }
}
