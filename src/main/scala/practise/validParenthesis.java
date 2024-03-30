package practise;

import java.util.LinkedList;
import java.util.Stack;

class validParenthesis {

    public static void main(String[] args) {
        System.out.println(isValid("]"));
    }
    public static boolean isValid(String s) {
        Stack<Character> stack=new Stack<>();

        for (int j=0;j<s.length();j++){
            char i= s.charAt(j);
            System.out.println(i);


            if(i==')'){

                if(stack.isEmpty())
                    return false;

                if(!stack.pop().equals('('))
                    return false;
            } else if(i==']'){

                if(stack.isEmpty())
                    return false;

                if(!stack.pop().equals('['))
                    return false;
            } else if(i=='}'){

                if(stack.isEmpty())
                    return false;

                if(!stack.pop().equals('{'))
                    return false;
            }
            else{
                stack.push(i);
            }
        }
        return stack.isEmpty();
    }



}