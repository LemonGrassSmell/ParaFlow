package cn.edu.ruc.iir.paraflow.examples;

import org.testng.annotations.Test;

import java.util.ArrayList;

public class ArrayListClearTest {
    @Test
    public void arrayListClearTest()
    {
        ArrayList<Integer> abc = new ArrayList<>();
        abc.add(1);
        abc.add(2);
        abc.add(3);
        abc.clear();
        abc = new ArrayList<>();
        abc.add(4);
        abc.add(5);
        abc.add(6);
        abc.clear();
    }
}
