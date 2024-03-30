package practise;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class stringman {
    public static void main(String[] args) {
        int[] arr= {-1,0,1,2,-1,-4,-2,-3,3,0,4};
        Arrays.sort(arr);
        System.out.println(Arrays.toString(arr));
        List<List<Integer>> result=new ArrayList<>();
        for(int i=0;i<arr.length;i++){

            if(i>0 && arr[i]==arr[i-1]){
                System.out.println("here!");
                continue;
            }

            int start=i+1;
            int end=arr.length-1;

            while(start<end){
                if(arr[i]+arr[start]+arr[end]>0){
                    end--;
                } else if (arr[i]+arr[start]+arr[end]<0) {
                    start++;
                } else {
                    result.add(Arrays.asList(arr[i],arr[start],arr[end]));
                    System.out.println(i+" "+start+" "+end);
                    start++;

                    while(arr[start]==arr[start-1] && start <end){
                        start++;
                    }

                }
            }
        }
        System.out.println(result);
    }
}
