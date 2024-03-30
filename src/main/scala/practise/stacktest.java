package practise;

import java.util.Stack;

public class stacktest {
    public static void main(String[] args) {

        String string="vege**tab*le";
        Stack<Character> stack=new Stack<>();

        for (int i = 0; i < string.length() ; i++) {

            char chr=string.charAt(i);

            if(chr=='*'){
                stack.pop();
            }else{
                stack.push(chr);
            }

            //"vege**tab*le => vetale

        }

        String string1="";
        for(Character i: stack){
            string1=string1+i;
        }

        System.out.println(string1);
    }
}
