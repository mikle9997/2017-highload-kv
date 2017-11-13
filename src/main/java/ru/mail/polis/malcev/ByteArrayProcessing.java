package ru.mail.polis.malcev;

import org.jetbrains.annotations.NotNull;

/**
 * Class with functions of processing byte array
 */
public class ByteArrayProcessing {

    /**
     * Trim zeros from the end of the array
     * @param inputArray - byte array
     * @return input byte array, but without null elements in the end
     */
    @NotNull
    public static byte[] trimArray(byte[] inputArray) {
        if (inputArray.length == 0) {
            return inputArray;
        }
        int i;
        for(i = inputArray.length - 1;  inputArray[i] == 0 && i >= 0; i--) {
            //nothing
        }
        byte[] outputArray = new byte[i + 1];
        for (int j = 0; j < outputArray.length; j++) {
            outputArray[j] = inputArray[j];
        }
        return outputArray;
    }
}
