package org.apache.hadoop.mapred.openflow;

public class InternetUtil {
    public static int toIPv4Address(String ipAddress) {
        if (ipAddress == null)
            throw new IllegalArgumentException("Specified IPv4 address must" + 
                "contain 4 sets of numerical digits separated by periods");
        String[] octets = ipAddress.split("\\.");
        if (octets.length != 4)
            throw new IllegalArgumentException("Specified IPv4 address must" +
                "contain 4 sets of numerical digits separated by periods");
        int result = 0;
        for (int i = 0; i < 4; ++i) {
            int oct = Integer.valueOf(octets[i]);
            if (oct > 255 || oct < 0)
                throw new IllegalArgumentException("Octet values in specified" +
                    " IPv4 address must be 0 <= value <= 255");
            result |=  oct << ((3-i)*8);
        }
        return result;
    }
    public static String fromIPv4Address(int ipAddress) {
        StringBuffer sb = new StringBuffer();
        int result = 0;
        for(int i=0; i < 4; ++i) {
            result = (ipAddress >> ((3-i)*8)) & 0xff;
            sb.append(Integer.valueOf(result).toString());
            if (i != 3)
                sb.append(".");
        }
        return sb.toString();
    }
}