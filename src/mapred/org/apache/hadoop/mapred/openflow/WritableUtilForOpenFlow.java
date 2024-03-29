package org.apache.hadoop.mapred.openflow;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

//all copy from org.apache.hadoop.io.WritableUtils and org.apache.hadoop.io.Text
public class WritableUtilForOpenFlow {
    ///////////////////
    // WritableUtils //
    ///////////////////
    public static void writeVInt(DataOutput stream, int i) throws IOException {
        writeVLong(stream, i);
    }
    public static void writeVLong(DataOutput stream, long i) throws IOException {
        if(i >= -112 && i <= 127) {
            stream.writeByte((byte)i);
            return;
        }

        int len = -112;
        if (i < 0) {
            i ^= -1L; // take one's complement'
            len = -120;
        }

        long tmp = i;
        while (tmp != 0) {
            tmp = tmp >> 8;
            len--;
        }

        stream.writeByte((byte)len);

        len = (len < -120) ? -(len + 120) : -(len + 112);

        for (int idx = len; idx != 0; idx--) {
            int shiftbits = (idx - 1) * 8;
            long mask = 0xFFL << shiftbits;
            stream.writeByte((byte)((i & mask) >> shiftbits));
        }
    }
    public static int readVInt(DataInput stream) throws IOException {
        return (int) readVLong(stream);
    }
    public static long readVLong(DataInput stream) throws IOException {
        byte firstByte = stream.readByte();
        int len = decodeVIntSize(firstByte);
        if(len == 1)
            return firstByte;
        long i = 0;
        for(int idx = 0; idx < len-1; idx++) {
            byte b = stream.readByte();
            i = i << 8;
            i = i | (b & 0xFF);
        }
        return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    }
    public static int decodeVIntSize(byte value) {
        if(value >= -112)
            return 1;
        else if(value < -120)
            return -119 - value;
        return -111 - value;
    }
    public static boolean isNegativeVInt(byte value) {
        return value < -120 || (value >= -112 && value < 0);
    }

    //////////
    // Text //
    //////////
    private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
        new ThreadLocal<CharsetEncoder>() {
            protected CharsetEncoder initialValue() {
                return Charset.forName("UTF-8").newEncoder().
                  onMalformedInput(CodingErrorAction.REPORT).
                  onUnmappableCharacter(CodingErrorAction.REPORT);
            }
        };
    private static ThreadLocal<CharsetDecoder> DECODER_FACTORY =
        new ThreadLocal<CharsetDecoder>() {
            protected CharsetDecoder initialValue() {
                return Charset.forName("UTF-8").newDecoder().
                  onMalformedInput(CodingErrorAction.REPORT).
                  onUnmappableCharacter(CodingErrorAction.REPORT);
            }
        };
    public static String readString(DataInput in) throws IOException {
        int length = WritableUtilForOpenFlow.readVInt(in);
        byte [] bytes = new byte[length];
        in.readFully(bytes, 0, length);
        return decode(bytes);
    }
    public static int writeString(DataOutput out, String s) throws IOException {
        ByteBuffer bytes = encode(s, true);
        int length = bytes.limit();
        WritableUtilForOpenFlow.writeVInt(out, length);
        out.write(bytes.array(), 0, length);
        return length;
    }
    public static ByteBuffer encode(String string, boolean replace)
      throws CharacterCodingException {
        CharsetEncoder encoder = ENCODER_FACTORY.get();
        if(replace) {
            encoder.onMalformedInput(CodingErrorAction.REPLACE);
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        ByteBuffer bytes =
          encoder.encode(CharBuffer.wrap(string.toCharArray()));
        if(replace) {
            encoder.onMalformedInput(CodingErrorAction.REPORT);
            encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return bytes;
    }
    public static String decode(byte[] utf8) throws CharacterCodingException {
        return decode(ByteBuffer.wrap(utf8), true);
    }
    private static String decode(ByteBuffer utf8, boolean replace)
      throws CharacterCodingException {
        CharsetDecoder decoder = DECODER_FACTORY.get();
        if(replace) {
            decoder.onMalformedInput(
              java.nio.charset.CodingErrorAction.REPLACE);
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
        }
        String str = decoder.decode(utf8).toString();
        // set decoder back to its default value: REPORT
        if(replace) {
            decoder.onMalformedInput(CodingErrorAction.REPORT);
            decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
        }
        return str;
    }
}
