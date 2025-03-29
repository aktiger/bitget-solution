package com.bitget.simulate.bigdecimal;


import java.util.Scanner;

public class SimulateBigDecimal {

    private long bot;
    private int exp;


    public SimulateBigDecimal() {
        bot = 0;
        exp = 0;
    }

    public SimulateBigDecimal(double d) {
        exp = 0;
        int flag = d > 0 ? 1 : -1;  // 取符号
        d = Math.abs(d);            // 取绝对值

        while (d < pow10(8) && d != 0) {  // 取8位精度
            d *= 10;
            exp--;
        }
        bot = flag * (long) (d);
    }

    public SimulateBigDecimal add(SimulateBigDecimal b) {
        this.specifyPrecision(7); // 都指定7位的精度，相加不会越界
        b.specifyPrecision(7);
        int dis = exp - b.exp;
        SimulateBigDecimal c = new SimulateBigDecimal();
        if (dis > 0) {
            c.bot = (long) (bot * Math.pow(10, dis)) + b.bot;
            c.exp = b.exp;
        } else {
            c.bot = bot + (long) (b.bot * Math.pow(10, -dis));
            c.exp = exp;
        }
        return c;
    }

    public SimulateBigDecimal subtract(SimulateBigDecimal b) {
        this.specifyPrecision(7); // 都指定7位的精度，相减不会越界
        b.specifyPrecision(7);
        int dis = exp - b.exp;
        SimulateBigDecimal c = new SimulateBigDecimal();
        if (dis > 0) {
            c.bot = (long) (bot * Math.pow(10, dis)) - b.bot;
            c.exp = b.exp;
        } else {
            c.bot = bot - (long) (b.bot * Math.pow(10, -dis));
            c.exp = exp;
        }
        return c;
    }

    public SimulateBigDecimal multiply(SimulateBigDecimal b) {
        this.specifyPrecision(4); // 都指定4位的精度，相乘不会越界
        b.specifyPrecision(3);
        SimulateBigDecimal c = new SimulateBigDecimal();
        c.bot = bot * b.bot;
        c.exp = exp + b.exp;

        return c;
    }

    public SimulateBigDecimal divide(SimulateBigDecimal b) {
        if (b.bot == 0) {
            System.out.println("The divider cannot be zero");
            return new SimulateBigDecimal();
        }
        this.specifyPrecision(7); // 被除数指定8位的精度
        b.specifyPrecision(4);    // 除数指定4位精度
        SimulateBigDecimal c = new SimulateBigDecimal();
        c.bot = bot / b.bot;
        c.exp = exp - b.exp;

        return c;
    }

    public SimulateBigDecimal assign(double d) {
        bot = 0;
        exp = 0;
        int flag = d > 0 ? 1 : -1;  // 取符号
        d = Math.abs(d);            // 取绝对值

        while (d < pow10(8) && d != 0) {  // 取8位精度
            d *= 10;
            exp--;
        }
        bot = flag * (long) (d);

        return this;
    }

    public double toDouble() {
        return bot * Math.pow(10, exp);
    }

    // 指定精度，即底数的位数
    public void specifyPrecision(int num) {
        if (bot == 0) return;
        int flag = bot > 0 ? 1 : -1;
        bot = Math.abs(bot);
        while (bot < pow10(num)) {
            bot *= 10;
            exp--;
        }
        while (bot > pow10(num + 1)) {
            bot /= 10;
            exp++;
        }
        bot = flag * bot;
    }

    private static double pow10(int exp) {
        return Math.pow(10, exp);
    }

    @Override
    public String toString() {
        return Double.toString(toDouble());
    }

    public static void main(String[] args) {
        SimulateBigDecimal a = new SimulateBigDecimal(50);
        SimulateBigDecimal b = new SimulateBigDecimal(8);
        System.out.println("a=" + a);
        System.out.println("b=" + b);
        System.out.println("a+b=" + a.add(b));
        System.out.println("a-b=" + a.subtract(b));
        System.out.println("a*b=" + a.multiply(b));
        System.out.println("a/b=" + a.divide(b));

        a.assign(-0.5);
        b.assign(-0.03);
        System.out.println("a=" + a);
        System.out.println("b=" + b);
        System.out.println("a+b=" + a.add(b));
        System.out.println("a-b=" + a.subtract(b));
        System.out.println("a*b=" + a.multiply(b));
        System.out.println("a/b=" + a.divide(b));

        a.assign(2.5);
        b.assign(4);
        System.out.println("a=" + a);
        System.out.println("b=" + b);
        System.out.println("(a*b)+(b-a)+(b/a)=" + (a.multiply(b).add(b.subtract(a)).add(b.divide(a))));

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Please input a (double): ");
            double t = scanner.nextDouble();
            a.assign(t);
            System.out.print("Please input b (double): ");
            t = scanner.nextDouble();
            b.assign(t);
            System.out.println("a=" + a);
            System.out.println("b=" + b);
            System.out.println("a+b=" + a.add(b));
            System.out.println("a-b=" + a.subtract(b));
            System.out.println("a*b=" + a.multiply(b));
            System.out.println("a/b=" + a.divide(b));
        }
    }
}