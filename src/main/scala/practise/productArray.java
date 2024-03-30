package practise;

import java.util.Arrays;

public class productArray {
    public static void main(String[] args) {
        // Driver Code
        int[] arr = {10, 3, 5, 6, 2};
        //            1, 10,30,150,900
        //                 ,180,300,900

        int[] productArray=new int[arr.length];
        int temp=1;

        for (int i = 0; i < arr.length; i++) {
            productArray[i]=temp;
            temp=temp*arr[i];
        }

        temp=1;

        for (int j = arr.length-1; j>=0; j--) {
            productArray[j]=temp*productArray[j];
            temp=temp*arr[j];
        }

        System.out.println(Arrays.toString(productArray));

    }
}
