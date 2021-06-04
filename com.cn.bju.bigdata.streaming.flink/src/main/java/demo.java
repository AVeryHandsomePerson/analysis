import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author ljh
 * @version 1.0
 */
public class demo {
    public static void main(String[] args) throws UnsupportedEncodingException {
        String decodeURL = URLDecoder.decode("timeIn%3D1622185082000%26timeOut%3D%26domain%3Dlocalhost%26url%3Dhttp%3A%2F%2Flocalhost%3A8869%2F%23%2F%26title%3D%E9%A6%96%E9%A1%B5%26referrer%3Dhttp%3A%2F%2Flocalhost%3A8869%2F%26sh%3D667%26sw%3D375%26cd%3D24%26lang%3Dzh-CN%26shopId%3D2000000111", "UTF-8");
        List<String> uriToList = Stream.of(decodeURL.split("&")).map(elem -> new String(elem))
                .collect(Collectors.toList());
        uriToList.stream().forEach(System.out::println);
        Map<String, String> uriToListToMap = new HashMap<>();
        for (String individualElement : uriToList) {
            uriToListToMap.put(individualElement.split("=",2)[0], individualElement.split("=",2)[1]);
        }
        GsonBuilder gsonMapBuilder = new GsonBuilder();
        Gson gsonObject = gsonMapBuilder.create();
        String uriToJSON = gsonObject.toJson(uriToListToMap);
        System.out.println(uriToJSON);
    }


}
