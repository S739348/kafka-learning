package com.aarya.kafkalearning.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data                   // generates getters, setters, toString
@AllArgsConstructor     // generates constructor with all fields
@NoArgsConstructor      // generates empty constructor
public class Order {
    private String id;
    private String item;
    private int    qty;
}