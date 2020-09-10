package com.sloth.gor;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.ArrayUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 流量记录中间件
 * @author sloth
 */
public class Echo {

    public static void main(String[] args) {
        if (args != null) {
            for (String arg : args) {
                System.out.println(arg);
            }
        }

        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        String line;
        log("start");

        // create some kind of unbounded blocking queue
        final LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<Message>();

        final Map<String, String[]> allContent = new ConcurrentHashMap<>();

        new Thread() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Message message = queue.take();

                        for (Map.Entry<String, String[]> it : allContent.entrySet()) {
                            String[] lstContent = it.getValue();
                            if (lstContent != null && !ArrayUtils.contains(lstContent, null)) {
                                for (int i=0; i < lstContent.length; i++) {
                                    if (i == 0) {
                                        log("Request type: Request");
                                    } else if (i == 2) {
                                        log("Request type: Replayed Response");
                                    }
                                    log(lstContent[i]);
                                    log("");
                                }
                                allContent.remove(it.getKey());
                            }
                        }

                        if (message.getTypeInfo().equals("request")) {
//                            log("<<<request>>>"+message.id+">>>>>>>>>>>>####");
                            if (allContent.containsKey(message.id)) {
//                                log("type:request>>messgeId:"+message.id);
                                String[] content = allContent.get(message.id);
                                if (content == null) {
                                    content = new String[3];
                                    content[0] = message.getOriginalContent();
                                } else {
                                    content[0] = message.getOriginalContent();
                                }
                                allContent.put(message.id,content);
                            } else {
//                                log("type:request>else>messgeId:"+message.id);
                                String[] content = new String[3];
                                content[0] = message.getOriginalContent();
                                allContent.put(message.id, content);
                            }
                        } else if (message.getTypeInfo().equals("response")) {
//                            log("<<<response>>>"+message.getOriginalContent()+">>>>>>>>>>>>####");
                            if (allContent.containsKey(message.id)) {
//                                log("type:response>>messgeId:"+message.id);
                                String[] content = allContent.get(message.id);
                                if (content == null ) {
                                    content = new String[3];
                                    content[1] = message.getOriginalContent();
                                } else {
                                    content[1] = message.getOriginalContent();
                                }
                                allContent.put(message.id,content);
                            } else {
//                                log("type:response>else>messgeId:"+message.id);
                                String[] content = new String[3];
                                content[1] = message.getOriginalContent();
                                allContent.put(message.id, content);
                            }

                        } else if (message.getTypeInfo().equals("replay")) {
//                            log("<<<replay>>>"+message.getOriginalContent()+">>>>>>>>>>>>####");
                            if (allContent.containsKey(message.id)) {
//                                log("type:replay>>messgeId:"+message.id);
                                String[] content = allContent.get(message.id);
                                if (content == null ) {
                                    content = new String[3];
                                    content[2] = message.getOriginalContent();
                                } else {
                                    content[2] = message.getOriginalContent();
                                }
                                allContent.put(message.id,content);
                            } else {
//                                log("type:replay>else>messgeId:"+message.id);
                                String[] content = new String[3];
                                content[1] = message.getOriginalContent();
                                allContent.put(message.id, content);
                            }
                        }

                        System.out.println(message.toHexMessage());
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();

        try {
            while ((line = stdin.readLine()) != null) {
                queue.offer(new Message(decodeHexString(line)));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String decodeHexString(String s) throws DecoderException {
        return new String(Hex.decodeHex(s.toCharArray()));
    }

    static void log(String s) {
        System.err.println(s);
    }

//    static String parseJobId(String body) {
//        // TODO would be a bit easier with a json lib, but for now it is sufficient
//        if (body.startsWith("{") && body.contains("\"job_id\"")) {
//            int index = body.indexOf("\"", body.indexOf(":"));
//            int toIndex = body.indexOf("\"", index + 1);
//            if (index >= 0 && toIndex > 0) {
//                String jobId = body.substring(index + 1, toIndex);
//                return jobId;
//            }
//        }
//        return "";
//    }

    static class Message {
        final String[] reqString;
        final String type;
        final String typeInfo;
        final String id;
        int enqueued = 0;
//        String jobId = "";
//        String status = "";
//        String path = "";

        public Message(String raw) {
            reqString = raw.split("\n");
            if (reqString.length < 2)
                throw new IllegalStateException("Illegal message format " + raw);

            type = reqString[0].split(" ")[0];
            id = reqString[0].split(" ")[1];
            // [2] => time (https://github.com/buger/goreplay/wiki/Middleware)
            // [3] => latency

            if ("3".equals(type)) {
                typeInfo = "replay";
//                status = reqString[1].split(" ")[1];
//                jobId = parseJobId(getBody());
            } else if ("2".equals(type)) {
                typeInfo = "response";
//                status = reqString[1].split(" ")[1];
//                jobId = parseJobId(getBody());
            } else if ("1".equals(type)) {
                typeInfo = "request";
                // GET /xy
//                path = reqString[1].split(" ")[1];
            } else {
                throw new IllegalStateException("unknown message type " + type);
            }
        }

        public String getTypeInfo() {
            return typeInfo;
        }

//        public String getBody() {
////            return reqString[reqString.length - 1];
//            List<String> rawList = Stream.of(reqString).map(obj -> Objects.toString(obj,null)).collect(Collectors.toList());
//            return rawList.stream().collect(Collectors.joining("\n"));
//        }

        public String getOriginalContent() {
            List<String> rawList = Stream.of(reqString).map(obj -> Objects.toString(obj,null)).collect(Collectors.toList());
            return rawList.stream().collect(Collectors.joining("\n"));
        }

        @Override
        public String toString() {
            return typeInfo + " " + id ;
        }

        public String toHexMessage() {
            return new String(Hex.encodeHex(toMessage().getBytes()));
        }

        public String toMessage() {
            StringBuffer sb = new StringBuffer();
            for (String msgPart : reqString) {
                sb.append(msgPart + "\n");
            }
            return sb.toString();
        }
    }
}
