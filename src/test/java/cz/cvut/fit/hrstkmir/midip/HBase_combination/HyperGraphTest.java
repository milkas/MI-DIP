/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.cvut.fit.hrstkmir.midip.HBase_combination;

import edu.uci.ics.jung.graph.SetHypergraph;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author mira
 */
public class HyperGraphTest {
    
    public HyperGraphTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }

    /**
     * Test of get method, of class HyperGraph.
     */
    @Test
    public void testGet() {
        System.out.println("get");
        HyperGraph instance = new HyperGraph();
        SetHypergraph<String, String> expResult = null;
        SetHypergraph<String, String> result = instance.get();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of createGraph method, of class HyperGraph.
     */
    @Test
    public void testCreateGraph() {
        System.out.println("createGraph");
        String file = "";
        HyperGraph instance = new HyperGraph();
        SetHypergraph<String, String> expResult = null;
        SetHypergraph<String, String> result = instance.createGraph(file);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
