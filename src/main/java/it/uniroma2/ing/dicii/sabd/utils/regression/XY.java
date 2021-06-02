package it.uniroma2.ing.dicii.sabd.utils.regression;


import scala.Serializable;

import java.sql.Date;


public class XY implements Serializable {
    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public void setX(double x) {
        this.x = x;
    }

    public void setX(Date x) {
        this.x = (double)x.getTime();
    }

    public void setY(double y) {
        this.y = y;
    }

    public void setY(Long y) {
        this.y = (double)y;
    }

    private double x;
    private double y;

    public XY(){
        this.x=0;
        this.y=0;
    }

    public XY(Double x, Long y){
        this.y = (double)y;
        this.x = x;
    }

    public XY(Long x, Long y){
        this.y = (double)y;
        this.x = (double)x;
    }

    public XY(Date x, Long y){
        this.y = (double)y;
        this.x = (double)x.getTime();
    }

}
